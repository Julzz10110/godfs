# Kubernetes production guide (goDFS)

Step-by-step setup for your **first production cluster** using only artifacts from this repository. Raft operations and membership are covered in [`deployments/k8s/OPERATIONS.md`](../deployments/k8s/OPERATIONS.md).

## Topology

| Component | Manifest | Role |
|-----------|----------|------|
| Namespace `godfs` | `deployments/k8s/base/namespace.yaml` | Isolation |
| Master Raft (5 voters) | `base/master-raft-statefulset.yaml`, `base/master-raft-services.yaml`, `base/master-raft-pdb.yaml` | Metadata + quorum |
| ChunkServer | `base/chunkserver.yaml` (+ PVC in production overlay) | Data plane |
| REST gateway | `base/restgateway.yaml`, `overlays/production/ingress-restgateway.yaml` | HTTP `/v1` for clients |
| Observability | `deployments/observability/*.yaml` | PrometheusRule + ServiceMonitor |
| DR (optional) | `deployments/k8s/dr/` | Snapshot backup/restore |

External clients: **Ingress → `godfs-restgateway:8080`**, TLS at the edge, **`Authorization: Bearer`** (see [`docs/EXTERNAL_ACCESS.md`](EXTERNAL_ACCESS.md)).

## Pre-production checklist

Before `kubectl apply` in production:

1. **Quorum:** at least **3** voters; baseline is **5** master pods (`GODFS_MASTER_PEERS` aligned with headless DNS).
2. **PDB:** `master-raft-pdb.yaml` — `minAvailable: 3`; do not drain more than two masters at once.
3. **Bootstrap:** `GODFS_MASTER_BOOTSTRAP=1` only on an **empty** cluster; after leader election set **0** (production overlay sets `0`).
4. **Raft PVC:** one RWO volume per pod; StorageClass with volume snapshots for backup.
5. **Chunk storage:** overlay uses PVC `godfs-chunk-data` (not `emptyDir` in prod).
6. **TLS:** Secret `godfs-tls` (cert/key/ca); `GODFS_TLS_RELOAD=1` for rotation without restart.
7. **Auth:** Secret `godfs-auth` — `GODFS_CLUSTER_KEY`, `GODFS_API_KEYS`, `GODFS_RBAC_JSON` (or ESO: `deployments/k8s/external-secrets/`).
8. **Images:** pin tags/registry in StatefulSet/Deployment; avoid unpinned `latest` in prod.
9. **Ingress:** set host/TLS secret in `overlays/production/ingress-restgateway.yaml`; tune timeouts/body size for max object size.
10. **Monitoring:** Prometheus Operator + `servicemonitors-godfs.yaml` + `prometheus-rules-godfs.yaml`; Grafana dashboard `deployments/observability/dashboards/godfs-overview.json`.
11. **Tracing (optional):** `OTEL_EXPORTER_OTLP_ENDPOINT` on pods.
12. **NetworkPolicy (optional):** example `deployments/k8s/network-policy-example.yaml` — verify CNI and ingress controller namespace.
13. **Sizing:** requests/limits in `overlays/production/patches/*`; tune for StorageClass and RPS.
14. **DR:** snapshot CronJob — `deployments/k8s/dr/README.md`; validate restore on staging.
15. **Runbook:** [`docs/RUNBOOK.md`](RUNBOOK.md) — leader loss, under-replicated, rebalance.
16. **Membership:** `AddMaster`/`RemoveMaster` procedure and smoke — [`scripts/k8s_raft_membership_smoke.sh`](../scripts/k8s_raft_membership_smoke.sh).
17. **Manifest gate:** `bash scripts/k8s_verify_manifests.sh` — `kubectl kustomize` + kubeconform (CI job `k8s-manifests`; no cluster required).

## First production cluster

### 0. Cluster preparation

- Kubernetes **≥ 1.28** (verified on **kind v0.24 / k8s 1.29** and **minikube v1.34 / k8s 1.30** — see [`OPERATIONS.md`](../deployments/k8s/OPERATIONS.md)).
- Ingress controller (e.g. ingress-nginx).
- (Recommended) Prometheus Operator for ServiceMonitor/PrometheusRule.

Build and load images (names must match manifest `image:` fields):

```bash
docker build -f deployments/docker/Dockerfile.master -t godfs-master:latest .
docker build -f deployments/docker/Dockerfile.chunkserver -t godfs-chunkserver:latest .
docker build -f deployments/docker/Dockerfile.restgateway -t godfs-restgateway:latest .
# kind: kind load docker-image godfs-master:latest ...
```

### 1. Secrets (TLS + auth)

Manually or via ESO:

```bash
kubectl apply -f deployments/k8s/external-secrets/godfs-secretstore-example.yaml  # when using ESO
kubectl apply -f deployments/k8s/external-secrets/godfs-secrets.yaml
```

Or create `godfs-tls` and `godfs-auth` in namespace `godfs` using the fields from `base/master-raft-statefulset.yaml`.

### 2. Master StatefulSet (initial bootstrap)

**First empty cluster:** apply the **base** kustomize (bootstrap enabled in `base/master-raft-statefulset.yaml`):

```bash
kubectl apply -k deployments/k8s
```

Wait until master pods are Ready and a leader is elected (logs / port-forward + `godfs-client masters list`).

**After bootstrap**, switch to the production overlay (bootstrap=0, resources, Ingress, observability):

```bash
# Edit host/TLS in deployments/k8s/overlays/production/ingress-restgateway.yaml
kubectl apply -k deployments/k8s/overlays/production
```

Validate without applying:

```bash
bash scripts/k8s_verify_manifests.sh
```

### 3. ChunkServer

Included in kustomize base/overlay. Ensure PVC `godfs-chunk-data` is Bound and the chunk pod is Ready.

### 4. REST gateway + Ingress

Gateway comes from the overlay; Ingress is `ingress-restgateway.yaml`. Check:

```bash
curl -sk -H "Authorization: Bearer <api-key>" https://godfs.example.com/v1/health
```

### 5. Prometheus rules + dashboard

Already included in the production overlay (`overlays/production/observability/`; keep in sync with `deployments/observability/` via `scripts/sync_observability_rules.sh`):

Import into Grafana: `deployments/observability/dashboards/godfs-overview.json`.

Validate rules locally: `bash scripts/observability_check.sh`.

### 6. Membership smoke

```bash
go build -o bin/godfs-client ./cmd/client
kubectl -n godfs port-forward svc/godfs-master 9090:9090 &
export GODFS_MASTER_ADDR=127.0.0.1:9090
# export GODFS_CLIENT_API_KEY=...  # from godfs-auth
bash scripts/k8s_raft_membership_smoke.sh
```

Lab add/remove (test clusters only):

```bash
export GODFS_MEMBERSHIP_LAB=1
export GODFS_MEMBERSHIP_LAB_RAFT_ADDR=godfs-master-lab@...
export GODFS_MEMBERSHIP_LAB_GRPC_ADDR=godfs-master-lab...:9090
bash scripts/k8s_raft_membership_smoke.sh
```

### 7. DR (as needed)

```bash
kubectl apply -f deployments/k8s/dr/snapshot-backup-cronjob.yaml
```

See [`deployments/k8s/dr/README.md`](../deployments/k8s/dr/README.md).

## Overlays

| Path | Purpose |
|------|---------|
| `deployments/k8s/` | Baseline via `base/` (5 masters, bootstrap=1 for first start) |
| `deployments/k8s/overlays/production/` | Prod: Ingress, SM/rules, chunk PVC, affinity, probes, bootstrap=0 |

## Helm (alternative)

```bash
helm install godfs deployments/helm/godfs -n godfs --create-namespace \
  --set prometheus.operator.enabled=true \
  --set raft.bootstrap=false
```

After the first bootstrap, keep `raft.bootstrap: false` in values.

## Related docs

- [`deployments/k8s/README.md`](../deployments/k8s/README.md)
- [`deployments/k8s/OPERATIONS.md`](../deployments/k8s/OPERATIONS.md)
- [`deployments/observability/README.md`](../deployments/observability/README.md)
