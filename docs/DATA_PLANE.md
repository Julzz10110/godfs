# goDFS data plane — replicas, repair, and GC

Operational model for **chunk replicas** on ChunkServers: how goDFS decides a replica is healthy, when background maintenance repairs or deletes data, and which environment variables control load. Normative repair policy is also in [ADR 003](adr/003-production2-data-plane-repair.md).

## Source of truth

| Concept | Rule |
|--------|------|
| **Committed content** | After a successful write, Master stores a **32-byte SHA-256** (`chunk_checksum_sha256`) on the chunk record. |
| **Stale replica** | Live replica whose on-disk SHA-256 (via `ChecksumChunk`) is 32 bytes and **≠** metadata digest. |
| **Not stale** | RPC error, missing digest, or digest length ≠ 32 — ignored for stale classification (best-effort). |
| **Repair** | When at least one **live** replica matches metadata and another live replica is stale → rebalancer runs **`PullChunk`** onto the stale target from a good peer. Primary placement role does **not** override checksum identity. |
| **Unrepairable** | No live replica matches metadata checksum → chunk marked unrepairable (`godfs_data_unrepairable_chunks`); operator action may be required. |

Helpers: `internal/dataplane` (`HasCommittedChunkChecksum`, `IsReplicaStaleComparedToMeta`).

## Background maintenance (Master leader)

| Loop | Env (examples) | Purpose |
|------|----------------|---------|
| **Rebalance** | `GODFS_REBALANCE_INTERVAL`, `GODFS_MAINT_*` in-flight limits | Under-replicated heal + stale repair |
| **Delete GC** | `GODFS_GC_INTERVAL`, `GODFS_GC_MAX_ATTEMPTS` | `DeleteChunk` for metadata-removed chunks |
| **Orphan GC** | `GODFS_ORPHAN_GC_INTERVAL` | Remove `*.chk` not referenced in metadata |
| **Health scan** | `GODFS_MAINT_HEALTH_SCAN_INTERVAL` (alias: `GODFS_STALE_REPLICA_GAUGE_INTERVAL`) | Full checksum scan → `godfs_data_stale_replicas` |

Health scan runs **only on the Raft leader** (single-master: always). Respect `GODFS_MAINT_CHECKSUM_MAX_QPS` and per-node checksum in-flight limits.

## Delete GC semantics

1. File delete in metadata enqueues **pending delete** per replica address.
2. GC issues `DeleteChunk` with backoff (`GODFS_GC_BACKOFF_*`, jitter).
3. Optional **`GODFS_GC_PENDING_DELETE_GRACE`**: defer first attempt until `created + grace`.
4. **`GODFS_GC_STRICT=1`**: after `GODFS_GC_MAX_ATTEMPTS`, **do not** drop the pending entry (default mode abandons it). Metric: `godfs_data_gc_strict_stuck`, counter `godfs_maint_gc_strict_hold_total`.
5. Pending set clears per address only after **successful** `DeleteChunk` on that address (chunk ID removed when all addresses acked).

## Metrics (selected)

| Metric | Meaning |
|--------|---------|
| `godfs_data_under_replicated_chunks` | Fewer live replicas than target RF |
| `godfs_data_stale_replicas` | Checksum mismatch vs metadata (health scan) |
| `godfs_data_pending_deletes` | Queued `DeleteChunk` actions |
| `godfs_data_unrepairable_chunks` | No good replica for repair |
| `godfs_maint_replica_meta_compare_total{result}` | `match`, `mismatch`, `rpc_error`, `short_checksum` |
| `godfs_data_gc_strict_stuck` | Pending deletes at max attempts (strict GC) |

## Namespace soft-delete (trash)

When **`GODFS_SOFT_DELETE_GRACE`** > 0 on Master:

- **`DELETE`** on a file sets a tombstone (`deleted_at`); the file is hidden from **list/stat/read/write** until grace expires or **`RestoreFile`** (gRPC admin, REST `POST /v1/fs/restore` with `{"path":"..."}`) clears it.
- After grace, the purge loop hard-deletes metadata and enqueues chunk **DeleteChunk** (normal GC).
- Directory deletes are immediate (not tombstoned).

## Write path failures (client / REST)

| Failure | Metadata effect |
|---------|-----------------|
| **Client disconnect** mid `WriteFromReader` / REST PUT | `CommitChunk` not called; file may exist with **size below** bytes sent. |
| **SyncChunk** to secondary fails or times out (RF&gt;1) | Primary `WriteChunk` returns error; **no commit**; size stays below payload. Orphan chunk bytes on disk may be removed by **orphan GC** when unreferenced. |

Tests: `test/integration/rest_partial_put_test.go`, `test/e2e/write_abort_test.go`, `test/e2e/syncchunk_timeout_test.go`.

## Operator actions

- **Stuck under-replicated:** check dead nodes, `godfs_maint_rebalance_errors_total`, run `godfs-client rebalance-run` (admin).
- **Stale / corruption:** inspect `godfs_data_stale_replicas`; rising count with live nodes often means checksum drift or failed repair — check `godfs_maint_replica_meta_compare_total{mismatch}` and rebalance errors ([RUNBOOK.md](RUNBOOK.md)).
- **Accidental file delete:** use **`RestoreFile`** / REST restore inside `GODFS_SOFT_DELETE_GRACE`.
- **Pending deletes:** restore chunk connectivity; use **`GODFS_GC_STRICT=1`** so entries are not dropped after max attempts (`godfs_data_gc_strict_stuck`).

## Related

- [RUNBOOK.md](RUNBOOK.md) — incidents and alerts  
- [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) — Production-2 baseline  
- E2E: `test/e2e/stale_repair_test.go`, `test/e2e/gc_test.go`, `test/e2e/syncchunk_timeout_test.go`
