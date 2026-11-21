#!/usr/bin/env bash
# Ubuntu 22.04/24.04. Run on VM1, VM2 and VM3 before kubeadm init/join.
set -euo pipefail
[[ $EUID -eq 0 ]] || { echo 'sudo bash scripts/prepare_kubeadm_vm.sh'; exit 1; }
swapoff -a
sed -i '/\sswap\s/s/^/# /' /etc/fstab
cat >/etc/modules-load.d/k8s.conf <<EOF
br_netfilter
overlay
EOF
modprobe br_netfilter
modprobe overlay
cat >/etc/sysctl.d/99-k8s.conf <<EOF
net.bridge.bridge-nf-call-iptables = 1
net.ipv4.ip_forward = 1
net.bridge.bridge-nf-call-ip6tables = 1
EOF
sysctl --system >/dev/null
apt-get update
apt-get install -y ca-certificates curl gpg containerd
mkdir -p /etc/containerd
containerd config default >/etc/containerd/config.toml
sed -i 's/SystemdCgroup = false/SystemdCgroup = true/' /etc/containerd/config.toml
systemctl enable --now containerd
systemctl restart containerd
mkdir -p -m 755 /etc/apt/keyrings
curl -fsSL https://pkgs.k8s.io/core:/stable:/v1.36/deb/Release.key \
 | gpg --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg
cat >/etc/apt/sources.list.d/kubernetes.list <<EOF
deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v1.36/deb/ /
EOF
apt-get update
apt-get install -y kubelet kubeadm kubectl
apt-mark hold kubelet kubeadm kubectl
systemctl enable --now kubelet
cat <<'EOT'
Prepared VM. On VM1: sudo kubeadm init --apiserver-advertise-address=<VM1_IP> --pod-network-cidr=192.168.0.0/16
On VM2/VM3: run kubeadm join command printed by VM1.
EOT
