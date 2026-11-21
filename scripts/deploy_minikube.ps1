<#
Windows PowerShell 5.1 / PowerShell 7. Safe to rerun: never deletes a Minikube profile or PVC.
Run from any directory: .\scripts\deploy_minikube.ps1
Use -SkipBuild only when the requested app image already exists inside Minikube.
#>
[CmdletBinding()]
param(
    [string]$Profile = 'banking-k8s',
    [string]$Image = 'banking-app:v3',
    [switch]$SkipBuild,
    [switch]$SkipMetrics,
    [string]$OperatorManifest = 'https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-1.30/releases/cnpg-1.30.1.yaml'
)
$ErrorActionPreference = 'Stop'
Set-Location (Resolve-Path (Join-Path $PSScriptRoot '..'))

foreach ($cmd in @('minikube','kubectl')) {
    if (-not (Get-Command $cmd -ErrorAction SilentlyContinue)) {
        throw "Missing command '$cmd'. Install it and reopen PowerShell."
    }
}

function Assert-Native([string]$Stage) {
    if ($LASTEXITCODE -ne 0) {
        throw "$Stage failed (exit $LASTEXITCODE). Deployment STOPPED; inspect the error above."
    }
}

function Wait-KubernetesApi {
    Write-Host '[preflight] Checking Kubernetes API /readyz ...'
    for ($attempt=1; $attempt -le 15; $attempt++) {
        try {
            $out = & kubectl --request-timeout=10s get --raw=/readyz 2>$null
            if ($LASTEXITCODE -eq 0 -and (($out -join "`n").Trim()) -eq 'ok') {
                Write-Host '[preflight] Kubernetes API healthy.'
                return
            }
        } catch { }
        Write-Host "[preflight] API not yet ready ($attempt/15) ..."
        Start-Sleep -Seconds 4
    }
    throw 'Kubernetes API did not become healthy. Check: minikube -p banking-k8s status; kubectl cluster-info; minikube logs --last-start-only'
}

# Determine whether a profile already exists, so an existing 1.37 cluster is not
# silently destroyed or forcibly downgraded. New profiles use a supported K8s version.
$profileJson = & minikube profile list --output=json
Assert-Native 'minikube profile list'
$profileData = ($profileJson -join "`n") | ConvertFrom-Json
$knownProfiles = @($profileData.valid) + @($profileData.invalid)
$existing = @($knownProfiles | Where-Object { $_ -and $_.Name -eq $Profile }).Count -gt 0
if ($existing) {
    Write-Host "[minikube] Reusing existing profile '$Profile' without deleting its data."
    & minikube start -p $Profile --driver=docker
} else {
    Write-Host "[minikube] Creating new profile '$Profile' on Kubernetes v1.36.0 (CNPG 1.30 supported)."
    & minikube start -p $Profile --driver=docker --kubernetes-version=v1.36.0 --cpus=6 --memory=10240 --disk-size=40g --cni=calico
}
Assert-Native 'minikube start'

if (-not $SkipMetrics) {
    & minikube -p $Profile addons enable metrics-server
    Assert-Native 'metrics-server addon enable'
}
& kubectl config use-context $Profile
Assert-Native 'kubectl context switch'
Wait-KubernetesApi

try {
    $verJson = & kubectl --request-timeout=20s get --raw=/version
    if ($LASTEXITCODE -eq 0) {
        $ver = ($verJson -join "`n") | ConvertFrom-Json
        if ($ver.gitVersion -match '^v1\.37') {
            Write-Warning 'This existing profile runs Kubernetes 1.37; CloudNativePG 1.30 supports Kubernetes 1.34-1.36. Keep your data; see docs/assignment3/TROUBLESHOOTING_INSTALL_FA.md for a safe new-profile option.'
        }
    }
} catch { Write-Warning 'Could not read the Kubernetes server version.' }

Write-Host '[operator] Installing CloudNativePG 1.30.1 ...'
& kubectl --request-timeout=90s apply --server-side -f $OperatorManifest
Assert-Native 'CloudNativePG operator apply'
& kubectl -n cnpg-system rollout status deployment/cnpg-controller-manager --timeout=300s
Assert-Native 'CloudNativePG operator rollout (check ImagePullBackOff and registry access)'

if ($SkipBuild) {
    $images = & minikube -p $Profile image ls
    Assert-Native 'minikube image ls'
    if (-not ($images -match ([regex]::Escape($Image)))) {
        throw "-SkipBuild was requested but '$Image' is not in Minikube. Build or load the image first."
    }
    Write-Host "[image] Reusing existing '$Image' in Minikube."
} else {
    Write-Host '[image] Building application image. A pypi.org timeout indicates network/proxy trouble, not missing Flask.'
    & minikube -p $Profile image build -f Dockerfile.k8s -t $Image .
    Assert-Native 'Minikube image build (if pip timed out, see troubleshooting guide)'
}

Write-Host '[deploy] Applying banking manifests ...'
& (Join-Path $PSScriptRoot 'deploy_existing_cluster.ps1') -Image $Image
if (-not $?) { throw 'Banking deployment script failed.' }
Write-Host 'SUCCESS: Run kubectl -n banking port-forward svc/gateway 8000:8000'
