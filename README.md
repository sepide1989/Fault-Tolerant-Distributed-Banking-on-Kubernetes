# Assignment 3 — Fault-Tolerant Distributed Banking on Kubernetes

**Full Persian deployment and bonus guide:** [README_ASSIGNMENT3_FA.md](README_ASSIGNMENT3_FA.md)

This repository extends the supplied Assignment 2 distributed banking code. It preserves sharding, local idempotency, same-shard transfers, distributed PostgreSQL prepared transactions (2PC), and optional B→C live re-sharding. Assignment 3 adds Kubernetes workloads, real PostgreSQL replication/failover using CloudNativePG, a PostgreSQL-backed durable 2PC coordinator journal, HPA, network policies, observability, and disaster-recovery exercises.

> **Execution status:** Static YAML/Python/Bash validation and 5 structural unit tests passed. This environment has no Docker, Minikube, kubectl or PostgreSQL; actual cluster deployment, fault injection, S3 backup, and Docker Hub push require execution on your own infrastructure. Do not claim real experiment PASS until database audit artifacts are collected.

## Quickstart on Windows 11 / Minikube

```powershell
.\scripts\deploy_minikube.ps1
kubectl -n banking get clusters,pods,svc,pvc,hpa
kubectl -n banking port-forward svc/gateway 8000:8000
```

In a second terminal:

```powershell
py tests/assignment3/run_experiments.py normal
py tests/assignment3/run_experiments.py duplicate
py tests/assignment3/run_experiments.py concurrent
py tests/assignment3/run_experiments.py pod-failure
py tests/assignment3/run_experiments.py primary-failure
py tests/assignment3/run_experiments.py coordinator-failure
py tests/assignment3/audit.py --wait-recovery 180 --expected-total 1999500
```

Autoscaling, rolling update/rollback and enforced NetworkPolicy commands are documented in the full guide.

## Bonus I / II

For a real cluster of three Ubuntu VMs, use `scripts/prepare_kubeadm_vm.sh`, `scripts/install_three_vm_addons.sh`, then install the `k8s/bonus/01-three-vm-db.yaml` overlay with the `CLUSTER_MANIFEST` setting. A `local-path` StorageClass provisions physically independent worker-local PVCs; each database uses three instances and synchronous durability. Before any destructive fault experiment, capture topology and verify an off-worker surviving replica.

The disaster-recovery scripts provide guarded workflows for a **physically fenced and permanently destroyed worker VM**, encrypted etcd/PKI backups for a single-control-plane cluster, and restoration onto a replacement VM. S3 object-store backup templates are included. Recovery depends on healthy external resources and requires **real restore testing**. Never erase a PostgreSQL PVC or alter committed bank balances merely to make tests pass.

## Contents

| Location | Purpose |
|---|---|
| `bank/`, `gateway/`, `bonus/`, `db/` | Original banking platform plus Assignment 3 extensions |
| `gateway/coordinator_pg.py` | Durable PostgreSQL 2PC coordinator and idempotent recovery |
| `gateway/k8s_bootstrap.py` | Idempotent schema/seed initialization |
| `Dockerfile.k8s` | Shared bank/gateway/recovery image |
| `k8s/base/` | Minikube DB clusters, deployments, services, HPA, policies |
| `k8s/bonus/`, `k8s/backup/` | Three-VM placement, synchronous replicas, WAL archive and DR |
| `k8s/optional-resharding/` | Optional compatibility with previous B→C migration |
| `scripts/` | Deployment, registry, load, database and VM recovery operations |
| `tests/assignment3/` | Database-level audit, six automated scenarios and structural tests |
| `docs/assignment3/` | Technical report (DOCX/PDF/MD), 20-minute video plan, evidence checklist |

### Images and evidence

Build/push to your own Docker Hub account:

```bash
docker login
DOCKERHUB_USER=your-user bash scripts/build_push.sh
```

Update the deployed image tag accordingly. Publish your own GitHub repository only after verifying no secrets or VM-specific private keys are present. Use `reports/assignment3/` for audit logs and real experiment observations; no fabricated run results are included.

**Legacy source README:** [README_FA.md](README_FA.md) (Assignment 2; preserved for continuity).
