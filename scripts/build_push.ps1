param([Parameter(Mandatory=$true)][string]$DockerHubUser)
$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')
$image="docker.io/$DockerHubUser/banking-k8s:v3"
docker buildx build --platform linux/amd64 -f Dockerfile.k8s -t $image --push .
if($LASTEXITCODE -ne 0){ throw 'Docker Hub push failed' }
Write-Host "Docker Hub image: $image"
