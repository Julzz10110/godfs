# Release checklist (R1–R10)

Manual acceptance before tagging a release (for example **v1.0.0**). Automated CI gates are in [`docs/CI.md`](CI.md).

Run automated gates (CI job `release_automated_gate`):

```bash
bash scripts/release_automated_gate.sh
```

Run the full checklist script (CI job `release-checklist`; needs Docker on Linux):

```bash
go build -o bin/godfs-client ./cmd/client
bash scripts/release_checklist_run.sh
```

CI uses `RELEASE_R1_SIZE_MB=10` and `RELEASE_R2_HEAL_SEC=300` for speed; production release: defaults (**100 MiB**, **900 s** heal).

## Checklist

| ID | Step | How to verify | OK |
|----|------|---------------|-----|
| **R1** | Full compose stack: upload **100 MiB**, read back, SHA-256 match | `release_checklist_run.sh` (or manual raft compose + `godfs-client write/read`). Override size: `RELEASE_R1_SIZE_MB=100`. | ☑ script |
| **R2** | Kill one chunk → under-replicated → healing within **15 min** (lab) | `release_checklist_run.sh`; overlay `docker-compose.release-checklist.yml` sets `GODFS_NODE_DEAD_AFTER=5s` in CI. | ☑ script |
| **R3** | Kill Raft leader → new leader; metadata write succeeds | `compose_raft_leader_chaos.sh` (also `raft-compose` CI). | ☑ script |
| **R4** | REST presigned **GET** and **PUT** (CDN-style, no Bearer on HTTP) | `go test ./internal/restgateway/... -run Presign`; e2e `PresignedPUT` with stack up (`REST_GATEWAY`). | ☑ script |
| **R5** | mTLS + RBAC deny/allow | Unit: `RBAC`, `HTTPServerTLS` tests; **staging**: TLS + `GODFS_RBAC_JSON` deny/allow on REST/gRPC. | ☑ unit + staging |
| **R6** | Prometheus rules valid; alerts sane in test | `observability_check.sh` (`observability` CI). | ☑ script |
| **R7** | Snapshot create → restore (DR procedure) | E2e `BackupSnapshot`; prod: [`deployments/k8s/dr/README.md`](../deployments/k8s/dr/README.md). | ☑ e2e + DR doc |
| **R8** | Operator CLI: nodes, rebalance, under-replicated | `godfs-client nodes list`, `rebalance-run`, `chunks under-replicated`. | ☑ script |
| **R9** | FUSE (Linux): create, write, read, unlink | `go test ./cmd/fuse/...` (`test` CI on Linux). | ☑ script |
| **R10** | API key rotation **without** process restart | Unit `TestAuthReload_SwapsAPIKeys`; prod: `GODFS_API_KEYS=@file` + `GODFS_AUTH_RELOAD_INTERVAL`. | ☑ unit + prod |

## Before tagging

1. All **R1–R10** marked above (or waived with reason in release notes).
2. **CI green** on the release commit (`lint`, `test`, `rest-compose`, `raft-compose`, `k8s-manifests`, `observability`).
3. Update [`docs/RELEASE_NOTES.md`](RELEASE_NOTES.md) for this version (features, breaking changes, known limits).
4. Pin container image tags in manifests/Helm values used for production.

## Known limitations (document in release notes)

See [`docs/RELEASE_NOTES.md`](RELEASE_NOTES.md) — Linux-only FUSE, no macOS/WinFsp, replication model, etc.
