# Operational security and tooling

Goal: run the cluster on-call without hand-editing live environment variables and without downtime when rotating keys and certificates.

## What is in the repository

The following is implemented in code and docs; anything beyond this depends on your organization (per-peer gRPC limits, org-specific secrets).

| Item | Where / how |
|------|----------------|
| TLS hot-reload (Master/Chunk gRPC, mTLS CA, client cert) | `internal/security/tls_reload.go`, `GODFS_TLS_RELOAD` |
| Extra trusted CA (bridge / dual trust) | **`GODFS_TLS_EXTRA_CA_FILE`** — PEM appended to the pool from `GODFS_TLS_CA_FILE` (server mTLS and client roots) |
| HTTPS on REST + same reload cadence | `GODFS_REST_HTTPS_ENABLED`, `GODFS_REST_TLS_*`, `cmd/restgateway` |
| External Secrets examples | `deployments/k8s/external-secrets/*.yaml` |
| RBAC cookbook (operator vs user) | `docs/RUNBOOK.md`, `docs/SECURITY_COOKBOOK.md` |
| `cluster` principal limited to `node`/`admin` | `internal/security/rbac.go` (`PrincipalCluster`) |
| TLS / JWKS rotation in runbook | `docs/RUNBOOK.md` |
| Optional unary gRPC rate limit (RPS, burst) | `GODFS_GRPC_RATE_LIMIT_RPS`, `internal/security/grpc_ratelimit.go`; excluded: `RegisterNode`, `Heartbeat` |
| Per-peer gRPC rate limit | `GODFS_GRPC_PEER_RATE_LIMIT_RPS` / `_BURST`; key = mTLS CN or Bearer hash |
| Hot-reload API keys / cluster key | `GODFS_API_KEYS=@file`, `GODFS_CLUSTER_KEY=@file`, `GODFS_AUTH_RELOAD_INTERVAL` |
| RBAC from file + periodic reload without restarting Master | **`GODFS_RBAC_JSON=@/path/rules.json`** and **`GODFS_RBAC_RELOAD_INTERVAL`** (≥ `2s`) |
| Node diagnostics / forced rebalance | `godfs-client nodes`, `masters`, `snapshot`; **`godfs-client rebalance-run [--steps N]`** (RPC `RunRebalanceNow`, admin) |

## Scope (baseline)

- **External Secrets** as the source of truth (ESO / Vault / cloud secret manager) → Kubernetes `Secret`.
- **Hot-reload** without restart:
  - **TLS server cert/key** (Master/Chunk/REST) when secrets change.
  - **TLS CA** for mTLS (server-side client CA) when secrets change.
  - **Client TLS cert/key** (mTLS) for internal clients (master↔chunk, chunk↔chunk) when secrets change.
  - JWKS: the keyfunc library refreshes keys automatically; procedures and parameters are documented.
- **Least privilege**:
  - Operator admin RPCs (membership change, snapshot create/delete) are separate from end-user read/write/delete.
  - Cluster principal (`GODFS_CLUSTER_KEY`) — only `node`/`admin` in RBAC.
- **Runbook**: rotation, break-glass access, and basic checks.

## Implementation checklist

1) **TLS hot-reload**

- env:
  - `GODFS_TLS_RELOAD=1` (enable)
  - `GODFS_TLS_RELOAD_INTERVAL=5s` (default)
- server (Master/Chunk **gRPC** and **REST gateway HTTPS** when `GODFS_REST_HTTPS_ENABLED`):
  - use `tls.Config.GetCertificate` and `GetConfigForClient` for dynamic `ClientCAs`.
- client:
  - use `tls.Config.GetClientCertificate` for mTLS keys.

2) **ExternalSecrets examples (ESO)**

- `GODFS_CLUSTER_KEY` (opaque secret)
- `GODFS_API_KEYS` (opaque secret / file-mounted)
- `GODFS_RBAC_JSON` (config secret)
- TLS: `tls.crt` / `tls.key` / `ca.crt` as files mounted into the pod.

3) **RBAC roles cookbook**

- example JSON rules:
  - `operator` (admin + node) on `/`
  - `user` read/write/delete on `/data`

4) **Rotation procedures**

- TLS: update external secret → wait for Kubernetes Secret refresh → processes pick up material without restart.
- cluster key / api keys / RBAC JSON: baseline — via `@file` and (optional) periodic reload / rolling restart.

## JWKS rotation notes

JWT validation via JWKS is enabled with `GODFS_JWT_JWKS_URL`.
The project uses `github.com/MicahParks/keyfunc/v3` (`keyfunc.NewDefault`), which:

- caches the key set,
- refreshes JWKS from the URL (best-effort),
- survives IdP key rotation without goDFS restarts.

Recommendations:

- keep rotation overlap (old and new key in JWKS) for the TTL/refresh window,
- watch master logs for `Unauthenticated jwt: ...` during rotation.
