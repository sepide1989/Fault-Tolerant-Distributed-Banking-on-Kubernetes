#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
IMAGE=${IMAGE:-banking-app:v3}
CLUSTER_MANIFEST=${CLUSTER_MANIFEST:-k8s/base/01-clusters.yaml}
kubectl apply -f k8s/base/00-namespace.yaml
kubectl apply -f k8s/base/00-configmap.yaml
if ! kubectl -n banking get secret bank-db-credentials >/dev/null 2>&1; then
  DB_PASSWORD=$(python -c 'import secrets;print(secrets.token_urlsafe(32))')
  kubectl -n banking create secret generic bank-db-credentials --type=kubernetes.io/basic-auth \
    --from-literal=username=bank --from-literal="password=$DB_PASSWORD"
fi
kubectl apply -f "$CLUSTER_MANIFEST"
for db in bank-a bank-b bank-meta; do
  kubectl -n banking wait --for=condition=Ready "cluster/$db" --timeout=600s
done
# Replace only the image field's known placeholder in streamed manifests.
sed "s|banking-app:v3|$IMAGE|g" k8s/base/02-init-job.yaml | kubectl apply -f -
kubectl -n banking wait --for=condition=complete job/bank-init-v3 --timeout=450s
sed "s|banking-app:v3|$IMAGE|g" k8s/base/03-apps.yaml | kubectl apply -f -
kubectl apply -f k8s/base/04-services.yaml
kubectl apply -f k8s/base/05-hpa-pdb.yaml
kubectl apply -f k8s/base/06-networkpolicy.yaml
for deploy in bank-a bank-b gateway coordinator-recovery; do
  kubectl -n banking rollout status "deployment/$deploy" --timeout=400s
done
kubectl -n banking get pods,svc,hpa,pvc
