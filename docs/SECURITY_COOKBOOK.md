# Security cookbook

This is a practical guide for operators and API consumers. **Navigation:** see also [`GETTING_STARTED.md`](GETTING_STARTED.md).

## Authentication

goDFS supports:

- **Cluster key** (`GODFS_CLUSTER_KEY`): internal node-to-node auth (chunk/master).
- **API keys** (`GODFS_API_KEYS`): maps `principal:key` pairs.
- **JWT HS256** (`GODFS_JWT_HMAC_SECRET`): subject becomes principal.
- **JWT via JWKS** (`GODFS_JWT_JWKS_URL`): recommended for IdP-backed auth.

REST gateway expects **per-request** auth:

```bash
curl -H "Authorization: Bearer <token>" ...
```

## RBAC (least privilege)

Use `GODFS_RBAC_JSON` to bind principals to path prefixes and permissions.

Example:

```json
[
  {"principal":"operator","path_prefix":"/","permissions":["admin","node","read","write","delete"]},
  {"principal":"app","path_prefix":"/data","permissions":["read","write"]},
  {"principal":"auditor","path_prefix":"/data","permissions":["read"]}
]
```

Notes:

- `admin` is required for snapshot create/delete and Raft membership admin RPCs.
- `node` is required for node ops RPCs (RegisterNode/Heartbeat).

## TLS / mTLS

### Server-side TLS (Master/Chunk)

Set:

- `GODFS_TLS_ENABLED=1`
- `GODFS_TLS_CERT_FILE`, `GODFS_TLS_KEY_FILE`
- `GODFS_TLS_CA_FILE` (enables **mTLS**: server verifies client certs)
- Optional **`GODFS_TLS_EXTRA_CA_FILE`**: append PEM roots (dual CA / cross-sign bridge) to the same trust pool as `GODFS_TLS_CA_FILE` for both server `ClientCAs` and client `RootCAs`.

### REST gateway inbound HTTPS (optional)

Independent of the gRPC client settings to the master:

- `GODFS_REST_HTTPS_ENABLED=1`
- `GODFS_REST_TLS_CERT_FILE`, `GODFS_REST_TLS_KEY_FILE` (or fall back to `GODFS_TLS_CERT_FILE` / `GODFS_TLS_KEY_FILE` when REST-specific paths are empty)
- Optional mTLS for HTTP clients calling the gateway: `GODFS_REST_TLS_CA_FILE`

Hot reload for REST server cert/key and REST client CA uses the same `GODFS_TLS_RELOAD` / `GODFS_TLS_RELOAD_INTERVAL` as gRPC.

### Client-side TLS (internal clients and end-user SDK)

Set:

- `GODFS_TLS_CA_FILE`
- (optional mTLS) `GODFS_TLS_CLIENT_CERT_FILE`, `GODFS_TLS_CLIENT_KEY_FILE`

### Hot reload

- `GODFS_TLS_RELOAD=1`
- `GODFS_TLS_RELOAD_INTERVAL=5s`

Server supports hot reload for both:

- server cert/key
- mTLS ClientCAs (client verification CA)

Client supports hot reload for:

- mTLS client cert/key

### gRPC abuse limits (optional)

Master and Chunk unary RPCs (process-wide token bucket):

- `GODFS_GRPC_RATE_LIMIT_RPS` — requests/sec (omit or `0` to disable)
- `GODFS_GRPC_RATE_LIMIT_BURST` — token bucket burst (default `max(10, ceil(2*RPS))`)
- `GODFS_GRPC_PEER_RATE_LIMIT_RPS` / `GODFS_GRPC_PEER_RATE_LIMIT_BURST` — per-caller bucket (mTLS client CN, else Bearer token hash)
- `GODFS_API_KEYS=@/path/keys` and `GODFS_CLUSTER_KEY=@/path/key` — file-backed secrets; `GODFS_AUTH_RELOAD_INTERVAL` (≥ `2s`) hot-reloads without restart

On the **Master**, `RegisterNode` and `Heartbeat` are **exempt** so chunk clusters are not starved by the same bucket as user metadata RPCs. Streaming RPCs on Chunk are not limited by this env (baseline).

## REST gateway (abuse surface)

Optional process-wide token bucket for **incoming HTTP** (REST gateway):

- `GODFS_REST_RATE_LIMIT_RPS` — sustained requests per second (`0` or unset disables)
- `GODFS_REST_RATE_LIMIT_BURST` — burst size (defaults when RPS is set)

**CORS** (browser integrations): `GODFS_REST_CORS_ALLOW_ORIGINS` and related `GODFS_REST_CORS_*` variables; defaults are conservative (no origins until configured).

Upload and JSON body caps, streaming buffer sizes, HTTPS for the gateway are documented under **REST** in [`docs/EXTERNAL_ACCESS.md`](EXTERNAL_ACCESS.md).

**Presigned content URLs:** when `GODFS_REST_PRESIGN_HMAC_SECRET` is set on the gateway, clients can call `GET` or `PUT /v1/fs/content` with `godfs_exp` + `godfs_sig` (see EXTERNAL_ACCESS). Use **`GODFS_REST_PRESIGN_UPSTREAM_BEARER`** so the gateway can authenticate to a secured Master on behalf of anonymous HTTP clients.

## Secrets in Kubernetes (External Secrets baseline)

See examples:

- `deployments/k8s/external-secrets/godfs-secrets.yaml` → creates `Secret` `godfs-auth` and `godfs-tls`

Workloads:

- mount `godfs-tls` as files under `/etc/godfs/tls`
- read `godfs-auth` keys via env vars (`GODFS_CLUSTER_KEY`, `GODFS_API_KEYS`, `GODFS_RBAC_JSON`)

## Rotation playbook (high level)

- **TLS cert/key & server-side CA**: rotate in secret manager → ESO updates Secret → processes reload (no restart).
- **JWKS**: rotate keys on IdP with overlap → goDFS validates via cached/auto-refreshed JWKS.
- **API keys / cluster key**: baseline is rolling restart unless you add explicit reload logic for these env-backed values.
- **RBAC JSON file**: set `GODFS_RBAC_JSON=@/path/to/rules.json` and **`GODFS_RBAC_RELOAD_INTERVAL`** (e.g. `30s`) on Master to pick up edits without pod restart.

