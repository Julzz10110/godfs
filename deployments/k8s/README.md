# Kubernetes

**Raft cluster + chunk + gateway:** see [OPERATIONS.md](OPERATIONS.md) for bootstrap, rolling updates, membership (`AddMaster` / `RemoveMaster`), and PDB/PVC notes.

Apply everything with Kustomize:

```bash
kubectl apply -k deployments/k8s
```

Or apply manifests in order (single-node example):

```bash
kubectl apply -f namespace.yaml
kubectl apply -f master.yaml
kubectl apply -f chunkserver.yaml
kubectl apply -f restgateway.yaml
```

## Raft Masters (5 pods + PVC)

Manifests:

```bash
kubectl apply -f namespace.yaml
kubectl apply -f master-raft-services.yaml
kubectl apply -f master-raft-pdb.yaml
kubectl apply -f master-raft-statefulset.yaml
kubectl apply -f chunkserver.yaml
kubectl apply -f restgateway.yaml
```

### Bootstrap (first start only)

- `master-raft-statefulset.yaml` ships with `GODFS_MASTER_BOOTSTRAP=1` **enabled** to bootstrap an empty cluster.
- After the first successful leader election, **disable bootstrap** (set `GODFS_MASTER_BOOTSTRAP` to empty/remove it) and re-apply the manifest:

```bash
kubectl apply -f master-raft-statefulset.yaml
```

Subsequent restarts must rely on the persisted Raft state under `GODFS_MASTER_RAFT_DIR` (PVC).

### Rolling updates / restarts

- Keep quorum: do not restart more than 2 masters at once.
- PDB (`master-raft-pdb.yaml`) enforces `minAvailable: 3`.
- Recommended: update one pod at a time and verify a leader exists.

## External Secrets

Examples live under `deployments/k8s/external-secrets/`:

- `godfs-secretstore-example.yaml` (placeholder SecretStore)
- `godfs-secrets.yaml` (ExternalSecret → `godfs-auth` and `godfs-tls`)

Apply them before workloads (requires External Secrets Operator installed in the cluster):

```bash
kubectl apply -f deployments/k8s/external-secrets/godfs-secretstore-example.yaml
kubectl apply -f deployments/k8s/external-secrets/godfs-secrets.yaml
```

Workloads reference `godfs-auth` and `godfs-tls`. TLS hot reload is enabled via `GODFS_TLS_RELOAD=1`.

Build and load images into your cluster (names must match `image:` fields):

```bash
docker build -f deployments/docker/Dockerfile.master -t godfs-master:latest .
docker build -f deployments/docker/Dockerfile.chunkserver -t godfs-chunkserver:latest .
docker build -f deployments/docker/Dockerfile.restgateway -t godfs-restgateway:latest .
```

Production checklist:

- TLS certificates via `Secret` volumes; set `GODFS_TLS_*` accordingly.
- Cluster auth: `GODFS_CLUSTER_KEY` (and user keys / RBAC) from `Secret`.
- Raft: use `StatefulSet` for masters and PVC for `GODFS_MASTER_RAFT_DIR` (see `master-raft-*.yaml`).
- REST gateway (`godfs-restgateway`): HTTP on port **8080**, scrape **`godfs-restgateway:9091/metrics`** when `GODFS_METRICS_LISTEN` is set. Expose HTTP via `Ingress` or `LoadBalancer` and terminate TLS at the edge; clients send **`Authorization: Bearer …`** (see `docs/EXTERNAL_ACCESS.md`).
- Prometheus `ServiceMonitor` can scrape `godfs-master:9091/metrics`, `godfs-chunk:9091/metrics`, and the REST gateway metrics port when enabled.
- Tracing: set `OTEL_EXPORTER_OTLP_ENDPOINT` (gRPC, default port 4317) on pods.
