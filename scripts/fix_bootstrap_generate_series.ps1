<#
Repair Assignment 3 bootstrap's PostgreSQL generate_series(smallint, smallint) overload error.
This only deletes/recreates the FAILED Kubernetes Job, NOT any DB Cluster or PVC.
Run from the project root in Windows PowerShell after CloudNativePG and DB clusters are Ready.
#>
[CmdletBinding()]
param(
    [string]$Profile = 'banking-k8s-136',
    [string]$Image = 'banking-app:v3-bootstrapfix1'
)
$ErrorActionPreference = 'Stop'
Set-Location (Resolve-Path (Join-Path $PSScriptRoot '..'))

function Assert-Exit([string]$Step) {
    if ($LASTEXITCODE -ne 0) { throw "$Step failed (exit code $LASTEXITCODE). Stopped without altering DB data." }
}

$context = (@(& kubectl config current-context) -join '').Trim()
Assert-Exit 'Read current context'
if ($context -ne $Profile) {
    throw "Current kubectl context '$context' does not equal requested '$Profile'. Run: kubectl config use-context $Profile"
}
$health = (@(& kubectl --request-timeout=60s get --raw=/readyz) -join '').Trim()
Assert-Exit 'Kubernetes API readiness'
if ($health -ne 'ok') { throw 'Kubernetes API is not Ready.' }

$sourcePath = Join-Path (Get-Location) 'gateway/k8s_bootstrap.py'
$old = 'generate_series(%s,%s)'
$new = 'generate_series(%s::integer,%s::integer)'
$content = [System.IO.File]::ReadAllText($sourcePath)
if ($content.Contains($old)) {
    $content = $content.Replace($old, $new)
    [System.IO.File]::WriteAllText($sourcePath, $content, [System.Text.UTF8Encoding]::new($false))
    Write-Host '[patch] Corrected generate_series argument casts.'
} elseif ($content.Contains($new)) {
    Write-Host '[patch] Source code is already corrected.'
} else {
    throw 'Expected SQL expression not found; refusing to modify source.'
}

Write-Host "[build] Building $Image using Docker Desktop ..."
& docker build -f Dockerfile.k8s -t $Image .
Assert-Exit 'Docker build'
Write-Host "[image] Loading $Image into Minikube profile $Profile ..."
& minikube -p $Profile image load $Image
Assert-Exit 'Minikube image load'

$jobName = (@(& kubectl -n banking get job bank-init-v3 --ignore-not-found -o name) -join '').Trim()
Assert-Exit 'Query bootstrap job'
if ($jobName) {
    $succeeded = (@(& kubectl -n banking get job bank-init-v3 -o 'jsonpath={.status.succeeded}') -join '').Trim()
    Assert-Exit 'Query bootstrap job success'
    if ($succeeded -eq '1') {
        Write-Host '[init] Job already succeeded. Keeping it; proceeding with app deployment.'
    } else {
        Write-Host '[init] Deleting the FAILED bootstrap Job only. DB Clusters/PVCs are untouched.'
        & kubectl -n banking delete job bank-init-v3 --wait=true
        Assert-Exit 'Delete failed bootstrap job'
        $jobName = ''
    }
}
if (-not $jobName) {
    $yaml = [System.IO.File]::ReadAllText((Join-Path (Get-Location) 'k8s/base/02-init-job.yaml'))
    $yaml = $yaml.Replace('banking-app:v3', $Image)
    $yaml | & kubectl apply -f -
    Assert-Exit 'Create corrected bootstrap job'
}
Write-Host '[init] Waiting for bootstrap Job to complete ...'
& kubectl -n banking wait --for=condition=complete job/bank-init-v3 --timeout=600s
Assert-Exit 'Bootstrap job completion'

Write-Host '[deploy] Deploying banking services ...'
& (Join-Path $PSScriptRoot 'deploy_existing_cluster.ps1') -Image $Image
if (-not $?) { throw 'Application deployment failed; inspect the output.' }
Write-Host '[success] Repair and banking deployment completed.'
