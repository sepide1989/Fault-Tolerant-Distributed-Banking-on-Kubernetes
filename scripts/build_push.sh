#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
: "${DOCKERHUB_USER:?Set DOCKERHUB_USER to your real Docker Hub username}"
IMAGE="docker.io/${DOCKERHUB_USER}/banking-k8s:v3"
docker buildx build --platform linux/amd64 -f Dockerfile.k8s -t "$IMAGE" --push .
echo "Docker Hub image: $IMAGE"
echo "Deploy on a kubeadm cluster: IMAGE=$IMAGE bash scripts/deploy_existing_cluster.sh"
