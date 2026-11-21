#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
IMAGE=${IMAGE:-banking-app:v3}
kubectl apply -f k8s/optional-resharding/01-c-db.yaml
kubectl -n banking wait --for=condition=Ready cluster/bank-c --timeout=600s
sed "s|banking-app:v3|$IMAGE|g" k8s/optional-resharding/02-c-init.yaml | kubectl apply -f -
kubectl -n banking wait --for=condition=complete job/bank-c-init-v3 --timeout=450s
for file in 03-c-app.yaml 05-migrator.yaml; do
  sed "s|banking-app:v3|$IMAGE|g" "k8s/optional-resharding/$file" | kubectl apply -f -
done
kubectl apply -f k8s/optional-resharding/04-c-service-policy.yaml
kubectl -n banking rollout status deployment/bank-c --timeout=300s
kubectl -n banking rollout status deployment/migrator --timeout=300s
echo 'To start migration: kubectl -n banking port-forward svc/migrator 8050:8050'
echo 'Then POST localhost:8050/start and /cutover according to docs.'
