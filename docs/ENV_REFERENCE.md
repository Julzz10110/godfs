# Environment variables reference

Quick index for operators. HTTP/REST details: [`EXTERNAL_ACCESS.md`](EXTERNAL_ACCESS.md). Security: [`SECURITY_COOKBOOK.md`](SECURITY_COOKBOOK.md), [`OPERATOR_SECURITY.md`](OPERATOR_SECURITY.md).

## Master

| Variable | Purpose |
|----------|---------|
| `GODFS_MASTER_LISTEN` / `GODFS_MASTER_GRPC_LISTEN` | gRPC listen address |
| `GODFS_CHUNK_SIZE_BYTES` | Chunk size (default 64 MiB) |
| `GODFS_REPLICATION` | Target replica count (default 3) |
| `GODFS_MASTER_NODE_ID` | Raft node id |
| `GODFS_MASTER_RAFT_LISTEN` | Raft TCP address |
| `GODFS_MASTER_RAFT_DIR` | Raft log/state directory |
| `GODFS_MASTER_PEERS` | Comma-separated `id=raftAddr=grpcAddr` |
| `GODFS_MASTER_BOOTSTRAP` | `1` once on empty cluster only |
| `GODFS_NODE_DEAD_AFTER` | Chunk liveness timeout (metadata) |
| `GODFS_RBAC_JSON` / `@file` | RBAC rules; `@path` + `GODFS_RBAC_RELOAD_INTERVAL` for hot reload |
| `GODFS_API_KEYS` / `@file` | Client API keys |
| `GODFS_CLUSTER_KEY` / `@file` | Cluster principal key |
| `GODFS_AUTH_RELOAD_INTERVAL` | Hot-reload keys (≥ 2s) |
| `GODFS_TLS_*` | Server/client TLS; `GODFS_TLS_RELOAD=1` for cert rotation |
| `GODFS_GRPC_RATE_LIMIT_RPS` / `_BURST` | Global unary rate limit |
| `GODFS_GRPC_PEER_RATE_LIMIT_RPS` / `_BURST` | Per-peer unary limit (CN or Bearer hash) |
| `GODFS_METRICS_LISTEN` | Prometheus `/metrics` |
| `GODFS_MAINT_*` / `GODFS_GC_*` | Rebalance, GC, health scan (see RUNBOOK) |

## Chunk server

| Variable | Purpose |
|----------|---------|
| `GODFS_MASTER` | Master gRPC address |
| `GODFS_CHUNK_LISTEN` | Chunk gRPC listen |
| `GODFS_CHUNK_DATA` | Data directory |
| `GODFS_NODE_ID` | Chunk node id |
| `GODFS_ADVERTISE_ADDR` | Address registered with master (reachable by clients/peers) |
| `GODFS_GRPC_PEER_STREAM_MAX_CONCURRENT` | Max concurrent `ReadChunk`/`PullChunk` per peer (`ResourceExhausted` when exceeded) |
| `GODFS_GRPC_*` / `GODFS_TLS_*` / `GODFS_API_KEYS` | Same patterns as master where applicable |

## REST gateway

| Variable | Purpose |
|----------|---------|
| `GODFS_REST_LISTEN` | HTTP listen (default `:8080`) |
| `GODFS_MASTER` | Upstream master gRPC |
| `GODFS_REST_HTTPS_ENABLED` | TLS on gateway |
| `GODFS_REST_TLS_*` | Gateway TLS files |
| `GODFS_REST_MAX_BODY_BYTES` / `GODFS_REST_MAX_UPLOAD_BYTES` | Upload limits |
| `GODFS_REST_RATE_LIMIT_RPS` / `_BURST` | HTTP rate limit per Bearer/IP |
| `GODFS_REST_PRESIGN_HMAC_SECRET` | Presigned GET/PUT (`godfs_exp`, `godfs_sig`) |
| `GODFS_REST_PRESIGN_UPSTREAM_BEARER` | Bearer to master when HTTP client is anonymous |
| `GODFS_REST_MULTIPART_DIR` | Multipart staging directory |
| `GODFS_REST_MULTIPART_MAX_PARTS` | Max parts per upload (default 1000) |
| `GODFS_REST_MULTIPART_MAX_PART_BYTES` | Max bytes per part |
| `GODFS_REST_CORS_*` | CORS headers |
| `GODFS_REST_READ_TIMEOUT` / `GODFS_REST_WRITE_TIMEOUT` | Server timeouts (`0` = off) |

**Metrics:** `godfs_rest_http_*`, `godfs_rest_multipart_uploads_active`, `godfs_rest_multipart_parts_staged_bytes`.

## Client / FUSE

| Variable | Purpose |
|----------|---------|
| `GODFS_CLIENT_API_KEY` | Bearer for master (CLI, FUSE, SDK dial) |
| `GODFS_TLS_*` | Client TLS to master/chunk |
| FUSE flags | `--mountpoint`, `--prefix`, `--master` — see [`EXTERNAL_ACCESS.md`](EXTERNAL_ACCESS.md) §3 |

## CI / tests

| Variable | Purpose |
|----------|---------|
| `GODFS_TESTCONTAINERS` | Enable testcontainers job |
| `GODFS_RAFT_LEADER_CHAOS_TIMEOUT` | Raft chaos script timeout |
