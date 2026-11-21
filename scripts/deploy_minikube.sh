#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
PROFILE=${PROFILE:-banking-k8s}
export PROFILE
IMAGE=${IMAGE:-banking-app:v3}
OPERATOR_MANIFEST=${OPERATOR_MANIFEST:-https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-1.30/releases/cnpg-1.30.1.yaml}

echo "[preflight] Using profile=$PROFILE with Calico (enforced NetworkPolicy)"
# Preserve existing profiles; use CNPG-supported Kubernetes v1.36.0 for new profiles.
EXISTS=$(minikube profile list -o json | python3 -c 'import json,sys,os; p=os.environ.get("PROFILE","banking-k8s"); d=json.load(sys.stdin); print("yes" if any(x.get("Name")==p for k in ("valid","invalid") for x in d.get(k,[])) else "no")')
if [[ "$EXISTS" == "yes" ]]; then
  echo "Reusing existing Minikube profile without deleting data"
  minikube start -p "$PROFILE" --driver=docker
else
  echo "Creating new Minikube profile on Kubernetes v1.36.0"
  minikube start -p "$PROFILE" --driver=docker --kubernetes-version=v1.36.0 --cpus=6 --memory=10240 --disk-size=40g --cni=calico
fi
minikube -p "$PROFILE" addons enable metrics-server
kubectl config use-context "$PROFILE"
for attempt in $(seq 1 15); do
  if [[ "$(kubectl --request-timeout=10s get --raw=/readyz 2>/dev/null || true)" == "ok" ]]; then
    break
  fi
  if [[ "$attempt" == 15 ]]; then
    echo 'Kubernetes API is not ready after retries' >&2
    exit 1
  fi
  sleep 4
done
kubectl apply --server-side -f "$OPERATOR_MANIFEST"
kubectl -n cnpg-system rollout status deployment/cnpg-controller-manager --timeout=300s
minikube -p "$PROFILE" image build -f Dockerfile.k8s -t "$IMAGE" .
IMAGE="$IMAGE" bash scripts/deploy_existing_cluster.sh
echo 'Access: kubectl -n banking port-forward svc/gateway 8000:8000'
