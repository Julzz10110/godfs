# Security cookbook

This is a practical guide for operators and API consumers.

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

### Server-side TLS (Master/Chunk/REST)

Set:

- `GODFS_TLS_ENABLED=1`
- `GODFS_TLS_CERT_FILE`, `GODFS_TLS_KEY_FILE`
- `GODFS_TLS_CA_FILE` (enables **mTLS**: server verifies client certs)

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

