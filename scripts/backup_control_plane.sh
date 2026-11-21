#!/usr/bin/env bash
# Root on original kubeadm CP. Creates private encrypted snapshot+PKI bundle.
set -euo pipefail
umask 077
[[ $EUID -eq 0 ]] || { echo 'Run as root on the original control plane'; exit 1; }
: "${CP_BACKUP_PASSWORD_FILE:?Root-only passphrase file required}"
: "${CP_BACKUP_OUTDIR:?Absolute encrypted backup destination required}"
: "${ETCDUTL_BIN:=etcdutl}"
[[ $CP_BACKUP_OUTDIR = /* && -s $CP_BACKUP_PASSWORD_FILE ]] || exit 1
command -v "$ETCDUTL_BIN" >/dev/null
command -v openssl >/dev/null
mkdir -p "$CP_BACKUP_OUTDIR"
chmod 700 "$CP_BACKUP_OUTDIR"
task_tmp=$(mktemp -d /var/tmp/banking-cp-backup.XXXXXXXX)
task_stamp=$(date -u +%Y%m%dT%H%M%SZ)
snapshot_path="/var/lib/etcd/banking-snapshot-$task_stamp.db"
trap 'rm -rf -- "$task_tmp"; rm -f -- "$snapshot_path"' EXIT
mapfile -t etcd_ids < <(crictl ps --name etcd -q)
[[ ${#etcd_ids[@]} -eq 1 ]] || { echo 'STOP: require exactly one local running etcd'; exit 1; }
etcd_args=(--endpoints=https://127.0.0.1:2379 --cacert=/etc/kubernetes/pki/etcd/ca.crt
 --cert=/etc/kubernetes/pki/etcd/healthcheck-client.crt --key=/etc/kubernetes/pki/etcd/healthcheck-client.key)
crictl exec "${etcd_ids[0]}" etcdctl "${etcd_args[@]}" endpoint health
crictl exec "${etcd_ids[0]}" etcdctl "${etcd_args[@]}" snapshot save "$snapshot_path"
cp -- "$snapshot_path" "$task_tmp/snapshot.db"
"$ETCDUTL_BIN" snapshot status "$task_tmp/snapshot.db" -w json >"$task_tmp/snapshot-status.json"
# A restore into a disposable directory validates snapshot hash, not just file size.
"$ETCDUTL_BIN" snapshot restore "$task_tmp/snapshot.db" --data-dir="$task_tmp/restore-probe" \
 --name=restore-probe --initial-cluster=restore-probe=http://127.0.0.1:12380 \
 --initial-advertise-peer-urls=http://127.0.0.1:12380 --bump-revision=1000000000 --mark-compacted >/dev/null
files=(etc/kubernetes var/lib/kubelet/config.yaml var/lib/kubelet/pki var/lib/kubelet/kubeadm-flags.env)
for optional in etc/containerd/config.toml etc/default/kubelet etc/cni/net.d etc/crictl.yaml; do
 [[ ! -e /$optional ]] || files+=("$optional")
done
tar -C / -cf "$task_tmp/control-plane.tar" "${files[@]}"
hostname -s >"$task_tmp/hostname"
kubeadm version -o short >"$task_tmp/kubeadm-version"
kubelet --version >"$task_tmp/kubelet-version"
crictl exec "${etcd_ids[0]}" etcdctl version >"$task_tmp/etcd-version"
KUBECONFIG=/etc/kubernetes/admin.conf kubectl -n kube-system get configmap kubeadm-config -o yaml >"$task_tmp/kubeadm-config.yaml"
KUBECONFIG=/etc/kubernetes/admin.conf kubectl -n banking get clusters,pvc -o json >"$task_tmp/banking-state.json"
(cd "$task_tmp" && sha256sum snapshot.db control-plane.tar >SHA256SUMS)
archive="$CP_BACKUP_OUTDIR/control-plane-$task_stamp.tar.gz.enc"
tar -C "$task_tmp" -czf "$task_tmp/bundle.tar.gz" snapshot.db snapshot-status.json control-plane.tar \
 hostname kubeadm-version kubelet-version etcd-version kubeadm-config.yaml banking-state.json SHA256SUMS
openssl enc -aes-256-cbc -salt -pbkdf2 -iter 200000 -in "$task_tmp/bundle.tar.gz" \
 -out "$archive" -pass "file:$CP_BACKUP_PASSWORD_FILE"
sha256sum "$archive" >"$archive.sha256"
echo "Created encrypted snapshot/PKI bundle: $archive"
echo 'PARTIAL: copy archive+sha256 OFF this VM, verify hash and run restore dry-run on an isolated replacement.'
echo 'Do not destroy the control plane before off-VM decrypt/restore and data-plane evidence are verified.'
