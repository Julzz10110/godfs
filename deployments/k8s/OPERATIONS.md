# Kubernetes + Raft operations

This guide matches the manifests under `deployments/k8s/` and the Helm chart under `deployments/helm/godfs/`.

## One-shot apply (Kustomize)

```bash
kubectl apply -k deployments/k8s
```

Apply External Secrets (or hand-built `godfs-tls` / `godfs-auth`) **before** workloads if you use TLS and cluster auth.

## Components

| Piece | Role |
|-------|------|
| `godfs-master-hs` | Headless service: stable DNS per pod (`godfs-master-0`, …) for Raft + gRPC peer list |
| `godfs-master` | ClusterIP: client-facing gRPC to any ready master (followers redirect) |
| StatefulSet `godfs-master` | 5 replicas, PVC `raft` per pod at `GODFS_MASTER_RAFT_DIR` |
| `master-raft-pdb.yaml` | `minAvailable: 3` — do not drain more than two masters at once |
| `godfs-chunk` | ChunkServers; `GODFS_MASTER` points at `godfs-master:9090` |
| `godfs-restgateway` | HTTP `/v1` in front of master gRPC |

## First bootstrap

1. With a **new** cluster, `GODFS_MASTER_BOOTSTRAP=1` is set in `master-raft-statefulset.yaml` so all peers form one Raft group.
2. Wait until a leader is elected (`kubectl logs` / `grpcurl` ListMasters if you have admin access).
3. **Turn off bootstrap** for every subsequent rollout: set `GODFS_MASTER_BOOTSTRAP` to empty or `0`, then re-apply. Further restarts must use on-disk state under the PVC only.

## Rolling updates and restarts

- Prefer `kubectl rollout restart statefulset/godfs-master -n godfs` or image/tag changes and rely on `RollingUpdate`.
- Respect the PDB: keep **at least three** masters available while changing five voters.
- The StatefulSet uses **parallel** pod creation for faster cold start; during upgrades, watch quorum (`kubectl get pods -w`).
- **Canary-style upgrade** (optional): set `spec.updateStrategy.rollingUpdate.partition` to `N` so only pods with index ≥ N update; then lower the partition to roll the rest.

## preStop / termination

Pods use a short **preStop sleep** so the process can finish in-flight RPCs and Raft can observe the peer leaving before SIGKILL. Keep `terminationGracePeriodSeconds` ≥ preStop + graceful shutdown budget.

## Membership changes (add / remove master)

Raft membership is changed **only on the leader** via gRPC (admin RBAC):

- `ListMasters` — current configuration and leader id.
- `AddMaster` — supply `node_id`, `raft_address`, `grpc_address` (same triple format as `GODFS_MASTER_PEERS`).
- `RemoveMaster` — supply `node_id`. The server refuses to drop below **three voters**.

Operational pattern:

1. **Scale up**: increase StatefulSet replicas (or add a new StatefulSet name prefix only if you run a parallel group — normally you scale the same set). Ensure the new pod’s `GODFS_MASTER_PEERS` includes **all** voters (update manifest / Helm values), apply, wait for pod Ready.
2. **Join Raft**: from the leader, call `AddMaster` with the new pod’s stable DNS raft and gRPC URLs.
3. **Scale down**: call `RemoveMaster` for the retiring `node_id` **while the pod still runs or after it is gone**, then decrease replicas. Order: remove from Raft first when shrinking voters, then delete PVCs if you intend to destroy data.

For automation, use `grpcurl` or a small admin client with the same TLS and `Authorization` as production clients.

## PVCs and backups

- Each master pod has its own `raft` PVC; **do not** share one RWX volume across masters for Raft logs.
- Metadata backup: use snapshot RPC / `deployments/k8s/dr/` CronJob; restore procedures are in `deployments/k8s/dr/README.md`.
- For disaster recovery of **Raft data directory**, use volume snapshots on the PVC storage class or file-level backup while the member is stopped.

## TLS and secrets

- Workloads mount `godfs-tls` and optional `godfs-auth` keys. See `external-secrets/` for ESO wiring.
- With `GODFS_TLS_RELOAD=1`, rotating Secret data updates certs without pod restart.

## Helm

```bash
helm install godfs deployments/helm/godfs -n godfs --create-namespace
```

Set `raft.bootstrap: false` after the first successful bootstrap in production values files.

## Local verification (kind / minikube)

Manual checklist validated on:

| Environment | Kubernetes | Notes |
|-------------|------------|-------|
| **kind** v0.24.0 | **1.29.2** | `kind create cluster`; `kind load docker-image` for godfs images |
| **minikube** v1.34.0 | **1.30.0** | `minikube start`; use `minikube image load` |

Steps (lab):

1. Create cluster and load images (`deployments/k8s/README.md`).
2. Create dev TLS/auth secrets (self-signed cert + test API key in `godfs-auth`).
3. `kubectl apply -k deployments/k8s` — wait for 5/5 master Ready and chunk/gateway Ready.
4. Port-forward `svc/godfs-master 9090:9090`; run `bash scripts/k8s_raft_membership_smoke.sh`.
5. `kubectl apply -k deployments/k8s/overlays/production --dry-run=client` (or full apply after editing Ingress host).
6. Confirm PDB: `kubectl get pdb -n godfs`; voluntary eviction of 3 masters at once should be blocked.
7. Rolling restart one master: `kubectl delete pod godfs-master-2 -n godfs`; verify new leader within minutes (`masters list`).
8. Document any drift from this table when re-testing on a newer k8s minor.

Manifest gate (no cluster required):

```bash
bash scripts/k8s_verify_manifests.sh
```

## Related docs

- `docs/K8S_PRODUCTION.md` — pre-prod checklist and first-cluster order
- `scripts/k8s_raft_membership_smoke.sh` — ListMasters (+ optional lab add/remove)
- `deployments/k8s/README.md` — quick apply order
