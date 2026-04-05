# goDFS

A distributed file system written in Go (1.26+). The current implementation includes: Master with in-memory metadata (no Raft), ChunkServers storing chunks as files on disk, gRPC without mTLS, and **3× replication by default** (primary write + `SyncChunk` to secondaries). Set `GODFS_REPLICATION=1` for single-replica mode (dev).

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

- Master: `GODFS_MASTER_LISTEN`, `GODFS_CHUNK_SIZE_BYTES`, **`GODFS_REPLICATION`** (default `3`; requires at least that many registered ChunkServers).
- Chunk: `GODFS_MASTER`, `GODFS_CHUNK_LISTEN`, `GODFS_CHUNK_DATA`, `GODFS_NODE_ID`, **`GODFS_ADVERTISE_ADDR`** (must be reachable from the client and other ChunkServers).

For **3× replication**, run three ChunkServer processes with distinct `GODFS_NODE_ID`, `GODFS_CHUNK_DATA`, `GODFS_CHUNK_LISTEN`, and `GODFS_ADVERTISE_ADDR` (e.g. ports 8000, 8001, 8002), then start Master and use the client as usual.

## Architecture (MVP)

- **`internal/domain`** — File, Chunk, Node entities and errors.
- **`internal/adapter/repository/metadata`** — in-memory namespace + chunk mapping.
- **`internal/adapter/repository/chunk`** — on-disk chunk storage (`*.chk` files).
- **`internal/adapter/grpc`** — Master and Chunk gRPC services (from `api/proto`).
- **`pkg/client`** — SDK: `Create`, `Mkdir`, `Read`, `Write`, `Delete`, `Rename`, `Stat`, `List`.

Default chunk size: **64 MiB** (as in the specification).

## Testing

End-to-end tests (in-process Master + ChunkServers, no Docker):

```bash
go test ./test/e2e/...
```

Unit tests: `go test ./internal/...`

## License

Apache-2.0
