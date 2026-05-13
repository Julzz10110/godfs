# Getting started with goDFS

Use this page as a **single index** for running the stack, calling **HTTP/REST**, mounting **FUSE (Linux)**, and deploying to **Kubernetes**. Step-by-step copy-paste flows live in the linked guides.

## Choose your path

| Goal | Where to go |
|------|-------------|
| REST + Docker Compose on your machine | [`docs/API_QUICKSTART.md`](API_QUICKSTART.md) — *Quick start (REST, local)* |
| Python or `curl` examples | [`docs/API_QUICKSTART.md`](API_QUICKSTART.md) |
| Mount a directory over the namespace (Linux) | [`docs/API_QUICKSTART.md`](API_QUICKSTART.md) — *Quick start (FUSE)*; full contract in [`docs/EXTERNAL_ACCESS.md`](EXTERNAL_ACCESS.md) |
| Auth, TLS, RBAC, rotation | [`docs/SECURITY_COOKBOOK.md`](SECURITY_COOKBOOK.md); operator security checklist [`docs/OPERATOR_SECURITY.md`](OPERATOR_SECURITY.md) |
| Metrics, alerts, incidents | [`docs/RUNBOOK.md`](RUNBOOK.md) |
| API paths, env vars, HTTP semantics | [`docs/EXTERNAL_ACCESS.md`](EXTERNAL_ACCESS.md) |
| Helm install (masters, chunks, REST gateway) | [`docs/API_QUICKSTART.md`](API_QUICKSTART.md) — *Kubernetes deployment (Helm)*; chart `deployments/helm/godfs`; gateway-only — `deployments/helm/godfs-restgateway` |
| Plain Kubernetes manifests | `deployments/k8s/README.md`, `deployments/k8s/OPERATIONS.md` |
| gRPC CLI / SDK (no HTTP) | root [`README.md`](../README.md) — *Quick start* |

## Minimal local stack (gRPC only)

From the repository root:

```bash
go run ./cmd/master
```

In other terminals, point each ChunkServer at the master (see [`README.md`](../README.md)). For **3× replication**, run three chunk processes with distinct `GODFS_NODE_ID`, data dirs, and advertise addresses.

**Raft (recommended for multi-node deployments):** set `GODFS_MASTER_NODE_ID`, `GODFS_MASTER_RAFT_LISTEN`, `GODFS_MASTER_RAFT_DIR`, `GODFS_MASTER_PEERS`, and bootstrap once with `GODFS_MASTER_BOOTSTRAP=1`. If these are unset, the master runs as a **single in-memory** metadata node (fine for quick local development).

## Roadmap and planning docs

High-level status and backlog: [`docs/IMPLEMENTATION_PLAN.md`](IMPLEMENTATION_PLAN.md), diagram: [`docs/ROADMAP.md`](ROADMAP.md).
