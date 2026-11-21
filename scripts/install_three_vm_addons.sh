#!/usr/bin/env bash
# On VM1 after kubeadm init + both workers joined. Real two-worker local storage.
set -euo pipefail
kubectl apply -f https://raw.githubusercontent.com/projectcalico/calico/v3.31.7/manifests/calico.yaml
kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/v0.0.37/deploy/local-path-storage.yaml
kubectl -n local-path-storage rollout status deploy/local-path-provisioner --timeout=240s
# Kubernetes v1.36 supported by metrics-server v0.9.0.
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/download/v0.9.0/components.yaml
# kubeadm usually uses self-signed kubelet serving certificates. DEMO ONLY.
# For production, provision trusted kubelet serving certificates and remove this flag.
if [[ ${DEMO_INSECURE_KUBELET_TLS:-0} == 1 ]]; then
  kubectl -n kube-system patch deployment metrics-server --type=json \
    -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'
fi
kubectl apply --server-side -f https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-1.30/releases/cnpg-1.30.1.yaml
kubectl -n cnpg-system rollout status deployment/cnpg-controller-manager --timeout=300s
kubectl get storageclass
kubectl get apiservice v1beta1.metrics.k8s.io
printf 'Verify: kubectl top nodes; kubectl -n banking get pvc; then deploy bonus overlay.\n'
