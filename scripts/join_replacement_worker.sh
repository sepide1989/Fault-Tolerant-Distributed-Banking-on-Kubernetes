#!/usr/bin/env bash
# ONLY on a prepared clean replacement VM. Token is in private kubeadm config.
set -euo pipefail
mode=${1:---dry-run}
[[ $mode = --apply || $mode = --dry-run ]] || exit 1
: "${WORKER_JOIN_CONFIG:?Root-only kubeadm JoinConfiguration file required}"
: "${EXPECTED_KUBEADM_VERSION:?Exact original kubeadm version, verified live, required}"
: "${REPLACEMENT_HOSTNAME:?Explicit replacement worker name required}"
[[ $EUID -eq 0 && -s $WORKER_JOIN_CONFIG ]] || exit 1
[[ $(hostname -s) = "$REPLACEMENT_HOSTNAME" ]] || { echo 'STOP: unexpected host'; exit 1; }
[[ $(kubeadm version -o short) = "$EXPECTED_KUBEADM_VERSION" ]] || { echo 'STOP: version mismatch'; exit 1; }
[[ ! -e /etc/kubernetes/kubelet.conf && ! -e /etc/kubernetes/manifests/kube-apiserver.yaml ]] || {
 echo 'STOP: this VM is already a cluster member/control plane'; exit 1;
}
[[ $(stat -c '%a' "$WORKER_JOIN_CONFIG") = 600 && $(stat -c '%u' "$WORKER_JOIN_CONFIG") = 0 ]] || {
 echo 'STOP: join configuration must be root-owned, mode 600'; exit 1;
}
if [[ $mode = --dry-run ]]; then echo 'DRY RUN: clean replacement/version verified; kubeadm join not run'; exit 0; fi
kubeadm join --config "$WORKER_JOIN_CONFIG"
echo 'PARTIAL: worker joined. From the restored control plane label banking-worker=true; verify new independent local-path PVCs, replication, and full client-ledger audit.'
