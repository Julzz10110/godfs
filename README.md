# goDFS

A distributed file system in Go (1.26+): **Master** (metadata), **ChunkServers** (64 MiB chunks on disk), **3× replication** by default (primary + `SyncChunk` to secondaries). Use `GODFS_REPLICATION=1` for single-replica dev.

The master can run in two modes:

- **Single process**, in-memory metadata (no extra env) — fastest for local hacking.
- **Raft cluster** — set `GODFS_MASTER_NODE_ID`, `GODFS_MASTER_RAFT_LISTEN`, `GODFS_MASTER_RAFT_DIR`, `GODFS_MASTER_PEERS`, and one-time `GODFS_MASTER_BOOTSTRAP=1` for an empty cluster. See [`docs/IMPLEMENTATION_PLAN.md`](docs/IMPLEMENTATION_PLAN.md) (этап 2) and [`deployments/k8s/OPERATIONS.md`](deployments/k8s/OPERATIONS.md).

**Consumers of REST, FUSE, or Helm:** start at **[`docs/GETTING_STARTED.md`](docs/GETTING_STARTED.md)** (quick starts and links).

## Quick start

Master:

```bash
go run ./cmd/master
```

ChunkServer (registers with Master):

```bash
set GODFS_MASTER=127.0.0.1:9090
set GODFS_ADVERTISE_ADDR=127.0.0.1:8000
go run ./cmd/chunkserver
```

On Linux/macOS, use `export` instead of `set`:

```bash
export GODFS_MASTER=127.0.0.1:9090
export GODFS_ADVERTISE_ADDR=127.0.0.1:8000
go run ./cmd/chunkserver
```

CLI:

```bash
go run ./cmd/client --master 127.0.0.1:9090 mkdir /data
go run ./cmd/client --master 127.0.0.1:9090 create /data/hello.txt
go run ./cmd/client --master 127.0.0.1:9090 write /data/hello.txt ./local.txt
go run ./cmd/client --master 127.0.0.1:9090 read /data/hello.txt ./out.txt
```

Environment variables:

- Master: `GODFS_MASTER_LISTEN` or `GODFS_MASTER_GRPC_LISTEN`, `GODFS_CHUNK_SIZE_BYTES`, **`GODFS_REPLICATION`** (default `3`; requires at least that many registered ChunkServers).
- Chunk: `GODFS_MASTER`, `GODFS_CHUNK_LISTEN`, `GODFS_CHUNK_DATA`, `GODFS_NODE_ID`, **`GODFS_ADVERTISE_ADDR`** (must be reachable from the client and other ChunkServers).

For **3× replication**, run three ChunkServer processes with distinct `GODFS_NODE_ID`, `GODFS_CHUNK_DATA`, `GODFS_CHUNK_LISTEN`, and `GODFS_ADVERTISE_ADDR` (e.g. ports 8000, 8001, 8002), then start Master and use the client as usual.

## Architecture (overview)

- **`internal/domain`** — File, Chunk, Node entities and errors.
- **`internal/adapter/repository/metadata`** — in-memory namespace (single-master mode).
- **`internal/raftmeta`** — Raft-backed metadata and FSM (when Raft env is configured).
- **`internal/adapter/repository/chunk`** — on-disk chunk storage (`*.chk` files).
- **`internal/adapter/grpc`** — Master and Chunk gRPC services (from `api/proto`).
- **`pkg/client`** — SDK: `Create`, `Mkdir`, `Read`, `ReadRange`, `Write`, `WriteAt`, `Delete`, `Rename`, `Stat`, `List`.

Default chunk size: **64 MiB**.

## Testing

End-to-end tests (in-process Master + ChunkServers, no Docker):

```bash
go test ./test/e2e/...
```

Unit tests: `go test ./internal/...`

REST integration tests (requires a running REST gateway, e.g. `docker compose -f deployments/docker/docker-compose.yml up -d`):

```bash
go test ./test/integration -tags=integration -count=1
```

Static analysis locally (same linters as CI): install [golangci-lint](https://golangci-lint.run/welcome/install/) and run `golangci-lint run` from the repo root.

## External access

- **REST:** `cmd/restgateway` — HTTP `/v1` API (Bearer `Authorization`, streaming GET/PUT, multipart, metrics, OTel). See **`docs/EXTERNAL_ACCESS.md`**, **`docs/GETTING_STARTED.md`**, and **`deployments/k8s/restgateway.yaml`** for Kubernetes.
- **FUSE (Linux only):** `cmd/fuse` — mount a namespace prefix with `go run ./cmd/fuse --mountpoint /mnt/godfs --prefix /data` (requires `fusermount3`, `GODFS_MASTER`, optional `GODFS_CLIENT_API_KEY`). Flags: cache TTL, `--rpc-timeout`. Details: **`docs/EXTERNAL_ACCESS.md`** §3 and **`docs/GETTING_STARTED.md`**.

## License

Apache-2.0
