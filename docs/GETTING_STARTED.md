# Getting started with goDFS

Single entry point for running the stack, **HTTP/REST**, **FUSE (Linux)**, **Kubernetes**, operations, and release acceptance.

**Status:** feature-complete for production-style deployment. Before a release tag, complete [`RELEASE_CHECKLIST.md`](RELEASE_CHECKLIST.md) (R1–R10).

## Choose your path

| Goal | Where to go |
|------|-------------|
| REST + Docker Compose on your machine | [`API_QUICKSTART.md`](API_QUICKSTART.md) — *Quick start (REST, local)* |
| Python or `curl` examples | [`API_QUICKSTART.md`](API_QUICKSTART.md) |
| Mount a directory over the namespace (Linux) | [`API_QUICKSTART.md`](API_QUICKSTART.md) — *FUSE*; contract in [`EXTERNAL_ACCESS.md`](EXTERNAL_ACCESS.md) |
| Auth, TLS, RBAC, rotation | [`SECURITY_COOKBOOK.md`](SECURITY_COOKBOOK.md); [`OPERATOR_SECURITY.md`](OPERATOR_SECURITY.md) |
| Environment variables (all roles) | [`ENV_REFERENCE.md`](ENV_REFERENCE.md) |
| Metrics, alerts, incidents | [`RUNBOOK.md`](RUNBOOK.md) |
| Prometheus rules, ServiceMonitors, Grafana | [`deployments/observability/README.md`](../deployments/observability/README.md) |
| CI jobs and chaos/netem scripts | [`CI.md`](CI.md) |
| API paths, HTTP semantics, presign, multipart | [`EXTERNAL_ACCESS.md`](EXTERNAL_ACCESS.md) |
| Helm (masters, chunks, REST gateway) | [`API_QUICKSTART.md`](API_QUICKSTART.md) — *Kubernetes (Helm)*; charts `deployments/helm/godfs`, `deployments/helm/godfs-restgateway` |
| Production K8s (first cluster) | [`K8S_PRODUCTION.md`](K8S_PRODUCTION.md) |
| K8s day-2 (Raft, PDB, membership) | [`deployments/k8s/OPERATIONS.md`](../deployments/k8s/OPERATIONS.md), `deployments/k8s/README.md` |
| gRPC CLI / SDK (no HTTP) | [`README.md`](../README.md) — *Quick start* |
| Release acceptance before tag | [`RELEASE_CHECKLIST.md`](RELEASE_CHECKLIST.md); `bash scripts/release_automated_gate.sh` |
| Known limits / release notes | [`RELEASE_NOTES.md`](RELEASE_NOTES.md) |

## Minimal local stack (gRPC only)

From the repository root:

```bash
go run ./cmd/master
```

In other terminals, point each ChunkServer at the master (see [`README.md`](../README.md)). For **3× replication**, run three chunk processes with distinct `GODFS_NODE_ID`, data dirs, and advertise addresses.

**Raft (recommended for multi-node):** set `GODFS_MASTER_NODE_ID`, `GODFS_MASTER_RAFT_LISTEN`, `GODFS_MASTER_RAFT_DIR`, `GODFS_MASTER_PEERS`, and bootstrap once with `GODFS_MASTER_BOOTSTRAP=1`. Without Raft env, the master uses **in-memory** metadata (local dev only).
