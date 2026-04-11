# Kubernetes

Apply manifests in order:

```bash
kubectl apply -f namespace.yaml
kubectl apply -f master.yaml
kubectl apply -f chunkserver.yaml
```

Build and load images into your cluster (names must match `image:` fields):

```bash
docker build -f deployments/docker/Dockerfile.master -t godfs-master:latest .
docker build -f deployments/docker/Dockerfile.chunkserver -t godfs-chunkserver:latest .
```

Production checklist:

- TLS certificates via `Secret` volumes; set `GODFS_TLS_*` accordingly.
- Cluster auth: `GODFS_CLUSTER_KEY` (and user keys / RBAC) from `Secret`.
- Raft: replace single `Deployment` with `StatefulSet` for masters, persistent volumes for `GODFS_MASTER_RAFT_DIR`.
- Prometheus `ServiceMonitor` can scrape `godfs-master:9091/metrics` and `godfs-chunk:9091/metrics` when `GODFS_METRICS_LISTEN` is set.
- Tracing: set `OTEL_EXPORTER_OTLP_ENDPOINT` (gRPC, default port 4317) on pods.
