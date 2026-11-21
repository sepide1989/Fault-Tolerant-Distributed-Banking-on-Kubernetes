#!/usr/bin/env bash
# Default: validate/decrypt/restore ONLY into a private temporary directory.
# --apply is restricted to a clean replacement with same hostname+IP+versions.
set -euo pipefail
umask 077
mode=${1:---dry-run}
[[ $mode = --dry-run || $mode = --apply ]] || { echo 'Use --dry-run or --apply'; exit 1; }
[[ $EUID -eq 0 ]] || { echo 'Run as root'; exit 1; }
: "${CP_BACKUP_ARCHIVE:?Encrypted archive required}"
: "${CP_BACKUP_PASSWORD_FILE:?Passphrase file required}"
: "${ETCD_NAME:?Original etcd member name required}"
: "${ETCD_PEER_IP:?Original peer IPv4 address required}"
: "${ETCDUTL_BIN:=etcdutl}"
[[ $ETCD_NAME =~ ^[a-zA-Z0-9.-]+$ && $ETCD_PEER_IP =~ ^[0-9.]+$ ]] || exit 1
command -v "$ETCDUTL_BIN" >/dev/null
task_tmp=$(mktemp -d /var/tmp/banking-cp-restore.XXXXXXXX)
trap 'rm -rf -- "$task_tmp"' EXIT
openssl enc -d -aes-256-cbc -pbkdf2 -iter 200000 -in "$CP_BACKUP_ARCHIVE" \
 -out "$task_tmp/bundle.tar.gz" -pass "file:$CP_BACKUP_PASSWORD_FILE"
mkdir "$task_tmp/bundle"
tar -C "$task_tmp/bundle" -xzf "$task_tmp/bundle.tar.gz"
(cd "$task_tmp/bundle" && sha256sum -c SHA256SUMS)
"$ETCDUTL_BIN" snapshot status "$task_tmp/bundle/snapshot.db" -w json
[[ $(cat "$task_tmp/bundle/hostname") = "$ETCD_NAME" ]] || { echo 'STOP: archive member mismatch'; exit 1; }
"$ETCDUTL_BIN" snapshot restore "$task_tmp/bundle/snapshot.db" --data-dir="$task_tmp/restored-etcd" \
 --name="$ETCD_NAME" --initial-cluster="$ETCD_NAME=https://$ETCD_PEER_IP:2380" \
 --initial-advertise-peer-urls="https://$ETCD_PEER_IP:2380" \
 --bump-revision=1000000000 --mark-compacted
tar -tf "$task_tmp/bundle/control-plane.tar" >"$task_tmp/config-files"
for required in etc/kubernetes/pki/ca.crt etc/kubernetes/pki/ca.key etc/kubernetes/manifests/etcd.yaml \
 etc/kubernetes/manifests/kube-apiserver.yaml var/lib/kubelet/config.yaml; do
 grep -qx "$required" "$task_tmp/config-files" || { echo "Missing required archive member: $required"; exit 1; }
done
[[ $mode = --apply ]] || { echo 'PASS: snapshot hash, restore probe and required config members verified. No live state changed.'; exit 0; }
[[ ${CP_FENCED_CONFIRMED:-} = "$ETCD_NAME" && ${ISOLATED_REPLACEMENT:-} = 1 ]] || {
 echo 'STOP: physically fence original CP and use an isolated replacement; set CP_FENCED_CONFIRMED=<old name> ISOLATED_REPLACEMENT=1'; exit 1;
}
[[ $(hostname -s) = "$ETCD_NAME" ]] || { echo 'STOP: replacement hostname differs'; exit 1; }
ip -4 -o addr show | awk '{print $4}' | cut -d/ -f1 | grep -Fxq "$ETCD_PEER_IP" || { echo 'STOP: original IP absent'; exit 1; }
[[ $(kubeadm version -o short) = "$(cat "$task_tmp/bundle/kubeadm-version")" ]] || { echo 'STOP: kubeadm version mismatch'; exit 1; }
[[ ! -d /var/lib/etcd || -z $(ls -A /var/lib/etcd) ]] || { echo 'STOP: nonempty etcd; never overwrite'; exit 1; }
[[ ! -d /etc/kubernetes/manifests || -z $(ls -A /etc/kubernetes/manifests) ]] || { echo 'STOP: existing static manifests'; exit 1; }
systemctl stop kubelet
# Stage manifest files away from the watched directory until restore is complete.
mkdir "$task_tmp/config"
tar -C "$task_tmp/config" -xf "$task_tmp/bundle/control-plane.tar"
mkdir -p /var/lib/etcd
cp -a "$task_tmp/restored-etcd/." /var/lib/etcd/
mkdir -p /etc/kubernetes /var/lib/kubelet
mv "$task_tmp/config/etc/kubernetes/manifests" "$task_tmp/manifests"
cp -a "$task_tmp/config/etc/kubernetes/." /etc/kubernetes/
cp -a "$task_tmp/config/var/lib/kubelet/." /var/lib/kubelet/
# Keep the replacement's working runtime/CNI setup; compare archived config first.
if [[ -d /etc/kubernetes/manifests ]]; then rmdir /etc/kubernetes/manifests; fi
mv "$task_tmp/manifests" /etc/kubernetes/manifests
systemctl enable --now containerd kubelet
echo 'PARTIAL: Kubernetes startup initiated. No banking correctness claim yet.'
echo 'Verify /readyz, original namespace/Cluster/PVC UIDs, all workers, operators and actual PG roles.'
echo 'Reconcile stale CNPG state against surviving data before workload rollout; run full baseline+client-ledger audit.'
echo 'Create and delete a disposable stateless pod to prove scheduling/management. Never delete a DB PVC as a scheduling test.'
