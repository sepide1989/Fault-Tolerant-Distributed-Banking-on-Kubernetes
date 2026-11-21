<# Safe and restartable deployment to an existing Kubernetes cluster. #>
[CmdletBinding()]
param(
    [string]$Image = 'banking-app:v3',
    [string]$ClusterManifest = 'k8s/base/01-clusters.yaml'
)
$ErrorActionPreference = 'Stop'
Set-Location (Resolve-Path (Join-Path $PSScriptRoot '..'))

function Invoke-Kubectl {
    param([Parameter(Mandatory=$true)][string[]]$Arguments)
    & kubectl @Arguments
    if ($LASTEXITCODE -ne 0) { throw "kubectl $($Arguments -join ' ') failed (exit $LASTEXITCODE)" }
}

# Check API readiness before mutating any resources.
$apiHealth = & kubectl --request-timeout=20s get --raw=/readyz
if ($LASTEXITCODE -ne 0 -or (($apiHealth -join "`n").Trim()) -ne 'ok') {
    throw 'Kubernetes API is not healthy. Check kubectl cluster-info and Minikube status.'
}

Invoke-Kubectl -Arguments @('apply','-f','k8s/base/00-namespace.yaml')
Invoke-Kubectl -Arguments @('apply','-f','k8s/base/00-configmap.yaml')

# --ignore-not-found avoids PowerShell NativeCommandError for an absent Secret.
# A real API/auth/network error still fails instead of being mistaken for absence.
$foundSecret = & kubectl -n banking get secret bank-db-credentials --ignore-not-found -o name
if ($LASTEXITCODE -ne 0) { throw 'Could not query the bank-db-credentials Secret.' }
if (-not $foundSecret) {
    Write-Host '[secret] Creating bank-db-credentials (first deployment) ...'
    $pass = ([guid]::NewGuid().ToString('N') + [guid]::NewGuid().ToString('N'))
    Invoke-Kubectl -Arguments @('-n','banking','create','secret','generic','bank-db-credentials','--type=kubernetes.io/basic-auth','--from-literal=username=bank',"--from-literal=password=$pass")
} else {
    Write-Host '[secret] Reusing existing bank-db-credentials (no password rotation).'
}

Invoke-Kubectl -Arguments @('apply','-f',$ClusterManifest)
foreach ($db in @('bank-a','bank-b','bank-meta')) {
    Invoke-Kubectl -Arguments @('-n','banking','wait','--for=condition=Ready',"cluster/$db",'--timeout=600s')
}

# Apply initialization only when it has never been created. Jobs have immutable Pod templates;
# avoid accidental immutable-field errors and avoid rerunning initialization on a successful DB.
$existingJob = & kubectl -n banking get job bank-init-v3 --ignore-not-found -o name
if ($LASTEXITCODE -ne 0) { throw 'Cannot query bank-init-v3 Job.' }
if (-not $existingJob) {
    Write-Host '[init] Creating the initial database bootstrap job ...'
    (Get-Content k8s/base/02-init-job.yaml -Raw).Replace('banking-app:v3',$Image) | & kubectl apply -f -
    if ($LASTEXITCODE -ne 0) { throw 'Init job manifest failed.' }
} else {
    Write-Host '[init] Existing bootstrap job found; waiting for completion, not recreating it.'
}
Invoke-Kubectl -Arguments @('-n','banking','wait','--for=condition=complete','job/bank-init-v3','--timeout=450s')

(Get-Content k8s/base/03-apps.yaml -Raw).Replace('banking-app:v3',$Image) | & kubectl apply -f -
if ($LASTEXITCODE -ne 0) { throw 'Deployment manifest failed.' }
Invoke-Kubectl -Arguments @('apply','-f','k8s/base/04-services.yaml')
Invoke-Kubectl -Arguments @('apply','-f','k8s/base/05-hpa-pdb.yaml')
Invoke-Kubectl -Arguments @('apply','-f','k8s/base/06-networkpolicy.yaml')
foreach ($name in @('bank-a','bank-b','gateway','coordinator-recovery')) {
    Invoke-Kubectl -Arguments @('-n','banking','rollout','status',"deployment/$name",'--timeout=400s')
}
Invoke-Kubectl -Arguments @('-n','banking','get','pods,svc,hpa,pvc')
Write-Host 'SUCCESS: Banking manifests applied.'
