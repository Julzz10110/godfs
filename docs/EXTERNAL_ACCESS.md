# External access to goDFS

**Quick entry:** see also [`GETTING_STARTED.md`](GETTING_STARTED.md) for navigation across REST, FUSE, and Kubernetes.

This document states goals, options, and boundaries for **external access to the file API** on top of the existing gRPC stack (Master + Chunk). Repository rollout order: **HTTP/REST first**, then optionally **FUSE (Linux)**.

---

## 1. Goals

| Goal | Description |
|------|-------------|
| **Integrations** | Calls from browsers, scripts (curl, Python `requests`), services without a gRPC SDK. |
| **POSIX-like access** | Mount a namespace prefix into the Linux file tree (editors, `cp`, `rsync`). |
| **Unchanged semantics** | Namespace, chunks, replication, and security behave like the current client; the new layer is transport and ergonomics only. |

Architectural constraints:

- Large files are handled in **chunks**; REST and FUSE must either stream or buffer with limits.
- Authentication is the **same** as gRPC: `Authorization: Bearer …` (API key / JWT), plus optional mTLS on the HTTP/FUSE process or a reverse proxy.

---

## 2. Option A — HTTP/REST (priority)

### 2.1. Placement

- A separate process **`godfs-rest`** (or `restgateway`) listens on **HTTP** (default `GODFS_REST_LISTEN=:8080`) or **HTTPS** when `GODFS_REST_HTTPS_ENABLED` is set (see §5).
- It uses a **gRPC client** to Master and Chunk (like `pkg/client`) without duplicating chunk read/write business logic.
- It connects to Master via **`GODFS_MASTER`** (same as the CLI).

### 2.2. Security

- Every request: the **`Authorization: Bearer <token>`** header is forwarded into outgoing gRPC **metadata** (same idea as `GODFS_CLIENT_API_KEY` for the CLI).
- The gateway process **must not** set a global `GODFS_CLIENT_API_KEY`, or the header token and the environment will conflict; TLS to Master uses `GODFS_TLS_*` like any other client.

### 2.3. API versioning

- Prefix **`/v1/`** for a stable contract.
- Error responses: JSON `{"error":"..."}` and HTTP status codes aligned with gRPC (`404` → NotFound, `409` → AlreadyExists, etc., as mapped).

### 2.4. Resources

| Method | Path / parameters | Purpose |
|--------|---------------------|---------|
| `GET` | `/v1/health` | Liveness (no auth or optional). |
| `GET` | `/v1/fs/stat?path=` | File or directory attributes (JSON). |
| `GET` | `/v1/fs/list?path=` | Directory listing (JSON). |
| `POST` | `/v1/fs/mkdir` | Body: `{"path":"..."}`. |
| `POST` | `/v1/fs/file` | Body: `{"path":"..."}` — empty file. |
| `DELETE` | `/v1/fs?path=` | Delete (soft-delete when Master has `GODFS_SOFT_DELETE_GRACE`). |
| `POST` | `/v1/fs/restore` | Body `{"path":"..."}` — restore from trash (admin). |
| `POST` | `/v1/fs/rename` | Body: `{"old_path":"...","new_path":"..."}`. |
| `GET` | `/v1/fs/content?path=` | Download content (**streaming** response; `Range`: single range, suffix `bytes=-N`, multiple ranges → `multipart/byteranges`; `ETag`, `If-Range`, `If-None-Match`, `If-Modified-Since`). |
| `PUT` | `/v1/fs/content?path=` | Upload content (**body** is a byte stream; gateway reads without buffering the whole file in RAM, writes via `WriteFromReader` in chunks). |
| `POST` | `/v1/fs/multipart` | Start **S3-style multipart** upload: JSON `{"path":"/file"}`, response `upload_id` + `path`. |
| `GET` | `/v1/fs/multipart/{uploadId}/parts` | List uploaded parts: JSON `parts[]` with `part_number`, `size_bytes`, `etag` (`sha256-…`). |
| `PUT` | `/v1/fs/multipart/{uploadId}?partNumber=N` | Upload part **N** (raw body); response **204**, **`ETag`** header (`"sha256-<hex>"`). |
| `POST` | `/v1/fs/multipart/{uploadId}/complete` | Complete: JSON `{"parts":[{"part_number":1,"etag":"..."}]}` — `etag` optional (SHA-256 verification). Query **`overwrite=1`** — overwrite an existing file. |
| `DELETE` | `/v1/fs/multipart/{uploadId}` | Abort upload and delete staging. |
| `POST` | `/v1/snapshots` | Body: `{"label":"..."}` — create metadata snapshot. |
| `GET` | `/v1/snapshots` | List snapshots. |
| `GET` | `/v1/snapshots/{id}` | Get manifest (JSON). |
| `DELETE` | `/v1/snapshots/{id}` | Delete snapshot. |

**Limits:** for `PUT`, see **`GODFS_REST_MAX_UPLOAD_BYTES`** / **`GODFS_REST_MAX_BODY_BYTES`** in §5. For **`GET`**, the response is streamed in segments (`GODFS_REST_GET_STREAM_BYTES`, default **4 MiB** per internal read); gateway peak memory does not grow with file size like a single large buffer.

**Presigned GET/PUT (optional):** when **`GODFS_REST_PRESIGN_HMAC_SECRET`** is set (≥ 16 characters), `GET` or `PUT /v1/fs/content?path=…` may include **`godfs_exp`** (Unix expiry) and **`godfs_sig`** = hex(HMAC-SHA256(secret, string `v1|<METHOD>|<path>|<exp>`) where `METHOD` is `GET` or `PUT`). Without `Authorization`, the gateway rejects content requests when the secret is configured. If the cluster requires a Bearer to Master, set **`GODFS_REST_PRESIGN_UPSTREAM_BEARER`** — the token the gateway attaches to outgoing gRPC for presigned requests.

### 2.5. Snapshots

Snapshots are exposed over REST and proxy the corresponding Master RPCs.

---

## 3. Option B — FUSE (Linux)

### 3.1. Purpose

- A separate **`godfs-fuse`** binary mounts a namespace prefix at a mountpoint (`fuse` / `fusermount3`).
- Implementation: **`bazil.org/fuse`** or **`github.com/hanwen/go-fuse/v2`** (pure Go, no CGO — preferred for cross-compilation).

### 3.2. Operations

Minimum: `Getattr`, `Lookup`, `ReadDir`, `Create`, `Mkdir`, `Unlink`, `Rmdir`, `Rename`, `Read`, `Write`, `Flush`, `Release` (plus **`Fsync`** as a no-op with local attribute cache invalidation).

Writes follow the same path as the SDK: PrepareWrite → chunk gRPC → CommitChunk; reads: GetChunkForRead → ReadChunk stream.

**Setattr:** shrinking/extending size (**`FATTR_SIZE`**) uses `TruncateFile`. Owner/mode changes (**uid/gid/mode**) → **`EPERM`**. **atime/mtime-only** updates are accepted without RPC (server-side attributes are unchanged).

### 3.3. Limitations

- **Linux** only (or macOS with macFUSE — out of scope for the first iteration).
- **Windows:** WinFsp plus a separate layer, or rely on REST/WebDAV later.
- **Write path:** writes are buffered in the FUSE process until `Flush`/`Release`, then sent as `WriteAt` (fewer RPCs for editors that flush often). Unflushed data is visible to reads on the same open file handle.
- **Truncate / `O_TRUNC`:** supported via Master RPC `TruncateFile` (`Setattr` size, `O_TRUNC` on open/create). Sparse extend (size &gt; current without writing) returns zero-filled reads.
- **When to use FUSE vs REST:** FUSE for interactive POSIX tools on Linux (`cp`, editors, `find`); REST for automation, browsers/CDN (presigned URLs), and cross-platform clients. See [`docs/RUNBOOK.md`](RUNBOOK.md) for operations.

### 3.4. Rollout order

1. REST gateway in the repository and CI.  
2. FUSE read-only prototype (list + read).  
3. Full read-write FUSE.

### 3.5. Implementation status (repository)

- **Read-write FUSE** (`cmd/fuse`, Linux build tag, **`github.com/hanwen/go-fuse/v2`**): `Getattr`, `Setattr` (see limits above), `Lookup`, `Readdir`, `Open`, `Create`, `Mkdir`, `Unlink`, `Rmdir`, `Rename`, `Read`, `Write`, `Flush`, `Release`, `Fsync`.
- **Stat / Readdir / negative lookup** cache with TTL (flags `--cache-ttl`, `--negcache-ttl`, `--dircache-ttl`; **`0`** disables cache).
- **gRPC → `errno`** mapping (`cmd/fuse/errno_linux.go`).
- Optional **`--rpc-timeout`** — deadline per outgoing gRPC from a FUSE operation.

Example (Linux):

```bash
mkdir -p /mnt/godfs
GODFS_MASTER=127.0.0.1:9090 GODFS_CLIENT_API_KEY=... \
  go run ./cmd/fuse --mountpoint /mnt/godfs --prefix / --cache-ttl 2s
```

You need **`fusermount3`** / libfuse and permission to mount (often the `fuse` group, or `user_allow_other` in `/etc/fuse.conf` for multi-user).

---

## 4. Observability and deployment

- HTTP metrics (Prometheus, same `GODFS_METRICS_LISTEN` as other binaries): `godfs_rest_http_requests_total`, `godfs_rest_http_request_duration_seconds`, `godfs_rest_http_response_bytes_total`, `godfs_rest_http_requests_in_flight`.
- Tracing: `otelhttp` on the gateway ingress → outgoing gRPC with the same OTLP endpoint.
- Kubernetes: a dedicated Deployment for `godfs-restgateway` with Ingress TLS, secrets for `GODFS_TLS_*` and Bearer. Example manifest: **`deployments/k8s/restgateway.yaml`**.

---

## 5. Implementation status (repository)

### REST (production-oriented)

- Binary: `go run ./cmd/restgateway` (or build `restgateway`).
- **Correlation:** every response adds **`X-Request-ID`** (if the client sent one, it is preserved); JSON errors include **`request_id`** when an id is present in context.
- **`http.Server` timeouts** (`time.ParseDuration`, e.g. `30m`):
  - `GODFS_REST_READ_HEADER_TIMEOUT` — default **`10s`** (slowloris protection on headers).
  - `GODFS_REST_READ_TIMEOUT` / `GODFS_REST_WRITE_TIMEOUT` — default **`15m`** (SLA for reading/writing request and response bodies). Explicit **`0`**, **`off`**, or **`false`** disables them (as in `docker-compose` for smoke tests).
  - `GODFS_REST_IDLE_TIMEOUT` — default **`120s`**.
- **Master/Chunk audit:** outgoing gRPC carries **`x-request-id`** metadata from HTTP `X-Request-ID`; with **`GODFS_AUDIT_ENABLED=1`**, JSON audit lines include **`request_id`** when provided.
- **Memory on download:** `GET /v1/fs/content` without `Range` and `Range` responses stream in segments (`pkg/client.StreamRangeToWriter`); segment size is **`GODFS_REST_GET_STREAM_BYTES`** (default **4 MiB**, minimum **64 KiB**).
- **JSON bodies** (`POST` mkdir/file/rename, `POST` snapshots): limit **`GODFS_REST_MAX_JSON_BODY_BYTES`** (default **1 MiB**).
- Environment variables:
  - `GODFS_MASTER` — gRPC master address.
  - `GODFS_REST_LISTEN` — HTTP/HTTPS listen address (default `:8080`).
  - **Inbound HTTPS (optional):** `GODFS_REST_HTTPS_ENABLED=1` and `GODFS_REST_TLS_CERT_FILE` / `GODFS_REST_TLS_KEY_FILE` (or fallback to `GODFS_TLS_CERT_FILE` / `GODFS_TLS_KEY_FILE`). Optional **mTLS to the gateway:** `GODFS_REST_TLS_CA_FILE` (PEM of trusted client CAs). **Hot-reload** of those files uses the same `GODFS_TLS_RELOAD` and `GODFS_TLS_RELOAD_INTERVAL` as gRPC (see `docs/RUNBOOK.md`). Outbound gRPC to Master still uses **`GODFS_TLS_*`** for the client (independent of the listener).
  - `GODFS_CHUNK_SIZE_BYTES` — chunk size (must match Master).
  - `GODFS_REST_MAX_BODY_BYTES` — default cap for `PUT` when no separate upload limit is set (default 80 MiB); also guides small JSON requests.
  - `GODFS_REST_MAX_UPLOAD_BYTES` — explicit cap for **`PUT /v1/fs/content`** (bytes). If **unset**, `GODFS_REST_MAX_BODY_BYTES` is used. **`0`** or a negative value means **no upload size cap** (streaming + client chunking only).
  - **CORS (optional, enabled when allow-origins is set)**:
    - `GODFS_REST_CORS_ALLOW_ORIGINS` — CSV origins (e.g. `https://app.example.com`, or `*`).
    - `GODFS_REST_CORS_ALLOW_METHODS` — default `GET,HEAD,POST,PUT,DELETE,OPTIONS`.
    - `GODFS_REST_CORS_ALLOW_HEADERS` — default includes `Authorization`, `Range`, `If-*`.
    - `GODFS_REST_CORS_EXPOSE_HEADERS` — default includes `ETag`, `Last-Modified`, `Content-Range`, `Accept-Ranges`, `Content-Length`.
    - `GODFS_REST_CORS_ALLOW_CREDENTIALS` — `1`/`true` to add `Access-Control-Allow-Credentials: true`.
    - `GODFS_REST_CORS_MAX_AGE` — preflight max-age (seconds), default 600.
  - **Rate limiting (optional, enabled when rps is set)**:
    - `GODFS_REST_RATE_LIMIT_RPS` — requests per second per key (Bearer token, else IP).
    - `GODFS_REST_RATE_LIMIT_BURST` — burst (default 10).
    - `GODFS_REST_RATE_LIMIT_TTL_SECONDS` — in-memory bucket TTL (default 600).
  - TLS / auth: **`GODFS_TLS_*`** for outbound gRPC to Master (like the CLI); for **HTTPS** on the gateway itself use **`GODFS_REST_HTTPS_ENABLED`** and `GODFS_REST_TLS_*` (above).
  - **Multipart staging:** `GODFS_REST_MULTIPART_DIR` (directory for temporary parts; default under `os.TempDir()`), `GODFS_REST_MULTIPART_MAX_PARTS` (default **1000**), `GODFS_REST_MULTIPART_MAX_PART_BYTES` (defaults to **`GODFS_REST_MAX_UPLOAD_BYTES`** / `GODFS_REST_MAX_BODY_BYTES`).
- Authentication: `Authorization: Bearer …` on every request (except `GET /v1/health` unless you enforce auth at the ingress).
- Endpoints: see §2.4 (including `/v1/snapshots/...`, multipart).

#### Errors and status codes

- HTTP status is mapped from gRPC codes (e.g. `NotFound → 404`, `ResourceExhausted → 429`, `Unimplemented → 501`).
- Error JSON includes:
  - `error` — message (for 500, `"internal error"`),
  - `code` — stable code (currently derived from gRPC code),
  - `grpc_code` — original gRPC code (string),
  - `http_status` — HTTP status (number),
  - `request_id` — when present (see `X-Request-ID`).

#### CORS / preflight

- With CORS enabled, the gateway answers preflight (`OPTIONS` + `Access-Control-Request-Method`) with `204` and `Access-Control-Allow-*` headers.

#### Rate limiting

- When over limit: `429 Too Many Requests`, JSON body with `code=rate_limited`.

### FUSE

- Implemented: **`cmd/fuse`** (see §3.5). Build on Linux: `go build -o godfs-fuse ./cmd/fuse`. CI (`ubuntu-latest`) runs **`go build ./cmd/fuse`**; errno mapping unit test: `go test ./cmd/fuse` (Linux only).

---

## 6. Phase 7 acceptance checklist

- [x] REST: `/v1` API, streaming **GET**, **PUT**, multipart, JSON/upload limits, `X-Request-ID` → **`x-request-id`** on gRPC and in audit, timeouts (default read/write **15m**), Prometheus, OTel, e2e in `test/e2e/rest_gateway_test.go`.
- [x] CI: **`rest-compose`** job (`.github/workflows/ci.yml`) — `docker compose` + `scripts/rest_compose_smoke.sh` (**`python3`** required to parse JSON `upload_id`).
- [x] FUSE: `cmd/fuse` read-write per §3 (including TTL cache, errno, `Release`/`Fsync`); REST K8s example — `deployments/k8s/restgateway.yaml`.

---

## 7. Related files

- `api/proto/godfs/v1/godfs.proto` — Master/Chunk contract.
- `pkg/client` — reference read/write logic.
- `internal/security` — TLS and Bearer for clients.
