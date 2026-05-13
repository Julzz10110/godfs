# goDFS — operations runbook

## Component overview

- **Master** — namespace metadata, placement, Raft (when a cluster is configured).
- **ChunkServer** — on-disk chunk storage, gRPC data plane.
- **Client** — `cmd/client` and `pkg/client`.

## Observability

### Prometheus

- **`GODFS_METRICS_LISTEN`** — HTTP address for `/metrics` (e.g. `:9091` or `0.0.0.0:9091`).
- gRPC metrics: counters and latency histograms (`go-grpc-prometheus`).
- Scraping: add a Prometheus job for `host:9091/metrics` for each Master/Chunk process.
- **Master business metrics:** `godfs_metadata_files_total`, `godfs_metadata_dirs_total`, `godfs_metadata_chunks_total`, `godfs_metadata_logical_bytes` (updated in the background together with data-plane gauges; on the Raft leader — live namespace).

### Tracing (OpenTelemetry → Jaeger/Tempo)

- **`OTEL_EXPORTER_OTLP_ENDPOINT`** or **`GODFS_OTEL_EXPORTER_OTLP_ENDPOINT`** — OTLP gRPC collector (often `host:4317` without a scheme).
- **`OTEL_SERVICE_NAME`** / **`GODFS_OTEL_SERVICE_NAME`** — service name in traces (defaults `godfs-master` / `godfs-chunkserver` in binaries).
- Outbound gRPC clients automatically get the `otelgrpc` stats handler when an endpoint is set (distributed traces Master ↔ Chunk).
- **REST gateway:** `otelhttp` wrapper + the same dial options → one trace context continues to Master over gRPC (check span parentage `godfs.restgateway` → `godfs-master` in Tempo/Jaeger when OTLP is configured).
- **Child spans on Chunk:** `pkg/client` and Master maintenance loops use the same dial options with `otelgrpc`; with OTLP enabled on **Master, ChunkServer, and REST**, the collector shows outbound `PrepareWrite` / `ReadChunk` / `PullChunk` / `DeleteChunk` as children of the user request or background work (as long as context is not dropped).

## Common incidents

| Symptom | What to check |
|--------|----------------|
| `FailedPrecondition: not leader` | Request hit a Raft follower; use the leader address from the error or a Kubernetes Service that targets the leader. |
| No replication / stuck rebalance | Heartbeat, `GODFS_NODE_DEAD_AFTER`, Master logs, gRPC error metrics to Chunk. |
| `Unauthenticated` / `Permission denied` | `GODFS_CLUSTER_KEY`, user API keys, RBAC JSON, TLS client cert. |
| No traces in Jaeger | OTLP endpoint, network to collector, service name, `InitOTel` running before outbound gRPC. |
| Rising `godfs_data_pending_deletes` | Dead chunk nodes or DeleteChunk network errors; see `godfs_maint_delete_errors_total`. |
| Rising `godfs_maint_rebalance_queue_depth` / `godfs_maint_gc_delete_chunks_queued` | Many queued rebalance/GC tasks; check `godfs_maint_*_errors_total`, `GODFS_MAINT_*` limits, Raft leader. |
| **GodfsHealingMayBeStalled** alert | Under-replicated chunks but no `rebalance_actions` in the window; check leader, `GODFS_REBALANCE_INTERVAL`, in-flight limits; optionally run **`godfs-client rebalance-run`** (admin). |
| Rising `godfs_metadata_*` without expected traffic | File/chunk accumulation, test leaks; verify deletes and GC. |

### ChunkServer disk full

- Symptoms: heartbeat shows growing `used_bytes`, chunk write errors in logs, `Unavailable` to clients.
- Actions: free disk or add nodes; check PVC/hostPath; ensure `GODFS_NODE_DEAD_AFTER` matches your SLA.

### Suspected corruption / replica skew

- Metrics: `godfs_data_unrepairable_chunks`, `godfs_data_stale_replicas` (if periodic scan is enabled), `godfs_maint_replica_meta_compare_total`.
- Actions: compare to a snapshot manifest; restore — `deployments/k8s/dr/README.md`.

## Alerting (Prometheus)

### Where rules live

- `deployments/observability/prometheus-rules-godfs.yaml` (PrometheusRule for Prometheus Operator / kube-prometheus-stack).

### GodfsRaftNoLeader

**Meaning:** no master reports `godfs_raft_is_leader=1`.

**What to do:**

- Check master pod reachability and networking (Raft port, default 9200).
- Ensure enough voters are up (quorum).
- Inspect master pod logs: election/timeout/appendEntries errors.

### GodfsUnderReplicatedChunks / GodfsChunkNodesDead

**Meaning:** under-replicated chunks exist or chunk nodes are dead by heartbeat.

**What to do:**

- Check chunkserver pods (restarts, OOM, disk, network errors).
- Check `GODFS_NODE_DEAD_AFTER` and heartbeat frequency.
- Inspect `godfs_maint_rebalance_errors_total` and gRPC error rates to chunk.

### GodfsPendingDeletesBacklog

**Meaning:** `godfs_data_pending_deletes` stays high (typical rule threshold — >1000 for 15m).

**What to do:**

- Check chunk node reachability and `godfs_maint_delete_errors_total`.
- Ensure the Master GC tick is running (Raft leader, `GODFS_GC_INTERVAL`).

### GodfsRebalanceQueueHigh

**Meaning:** `godfs_maint_rebalance_queue_depth` stays above the threshold (rules use >200).

**What to do:**

- Check `godfs_maint_rebalance_errors_total` and chunk reachability for `PullChunk`.
- Check limits `GODFS_MAINT_REBALANCE_INFLIGHT`, `GODFS_MAINT_PER_NODE_PULL_INFLIGHT`.

### GodfsHealingMayBeStalled

**Meaning:** under-replicated chunks exist but no `godfs_maint_rebalance_actions_total` events in 20m.

**What to do:**

- Ensure the leader process is healthy and `GODFS_REBALANCE_INTERVAL` is non-zero.
- After fixing the root cause, you may run **`godfs-client rebalance-run --steps N`** (admin).

### GodfsUnrepairableChunks

**Meaning:** the rebalancer found no good replica matching the metadata checksum.

**What to do:**

- Treat as potential corruption/skew: inspect chunk files on nodes, write paths.
- In test environments you may recreate data; in production you likely need manual restore from backup/snapshot.

## Metadata recovery (DR)

Goal: restore a **consistent namespace** after Raft disk loss, logical mistakes, or migration.

1. **Stop writes** to the cluster (ingress/clients); keep chunk nodes reachable for disk reads if needed.
2. **Take the last snapshot** before the incident (if any): `godfs-client snapshot get <id> manifest.json` or from external backup storage.
3. **Empty namespace + restore:** on the leader run `godfs-client snapshot restore manifest.json` (without `--force`, restore is only allowed into an empty namespace; with `--force` it overwrites metadata — **dangerous**, follow your org procedure).
4. **Raft / PVC:** after restore verify quorum; see **`deployments/k8s/dr/README.md`** and **`deployments/k8s/OPERATIONS.md`** (bootstrap, snapshot directory `GODFS_MASTER_RAFT_DIR`).
5. **Verify:** compare the manifest to chunks on nodes; on mismatch use data-plane repair or manual removal of stray chunks (orphan GC after metadata is aligned).

Operational nudge for healing without config changes: **`godfs-client rebalance-run --steps N`** (admin, leader-only) — runs up to N plan+execute rebalance steps.

## Deployment

- Docker: `deployments/docker/docker-compose.yml`.
- Kubernetes: `kubectl apply -k deployments/k8s` (Kustomize: Raft StatefulSet + chunk + gateway); bootstrap / rolling / membership — **`deployments/k8s/OPERATIONS.md`**. Single-master example: `deployments/k8s/master.yaml`.

## Raft control plane

### Membership change (add/remove masters)

CLI commands (leader-only; require `admin` in RBAC when auth is enabled):

```bash
godfs-client --master <leader:9090> masters list
godfs-client --master <leader:9090> masters add <node_id> <raft_addr> <grpc_addr>
godfs-client --master <leader:9090> masters remove <node_id>
```

Constraints and recommendations:

- Run changes **only through the leader** (otherwise `FailedPrecondition: not leader`).
- Do not drop voter count below a safe minimum (baseline: **3 voters**).
- For rolling restarts/upgrades keep quorum (for 5 voters: at least **3** available).

## REST gateway (`cmd/restgateway`)

- Variables and behavior: `docs/EXTERNAL_ACCESS.md` (§5).
- Default listener is **HTTP**. **HTTPS** on the same address (`GODFS_REST_LISTEN`): `GODFS_REST_HTTPS_ENABLED=1` + `GODFS_REST_TLS_CERT_FILE` / `GODFS_REST_TLS_KEY_FILE` (or fallback to `GODFS_TLS_CERT_FILE` / `GODFS_TLS_KEY_FILE`); optional mTLS for clients to the gateway — `GODFS_REST_TLS_CA_FILE`. Certificate hot-reload: `GODFS_TLS_RELOAD=1` (see “TLS rotation” below).
- Metrics: `GODFS_METRICS_LISTEN` — same `godfs_rest_*` counters as other binaries.
- Defaults: **`GODFS_REST_READ_TIMEOUT` / `GODFS_REST_WRITE_TIMEOUT` = 15m**; **`0`** / **`off`** disable them (in `deployments/docker/docker-compose.yml` the `rest` service sets **`0`** so smoke tests are not bounded).
- Docker Compose: `deployments/docker/docker-compose.yml` — services `master` (**`GODFS_REPLICATION=1`** for a single chunk), `chunk`, **`rest`** (image `Dockerfile.restgateway`). Smoke: **`bash scripts/rest_compose_smoke.sh`** (**`python3`** required on the host).
- Operator CLI: **`godfs-client nodes`** — registered chunk nodes and liveness (RPC `ListChunkNodes`, needs **admin** in RBAC when auth is on); **`godfs-client masters list|add|remove`**, **`godfs-client snapshot …`**, **`godfs-client rebalance-run [--steps N]`** (admin, RPC `RunRebalanceNow`).

## FUSE (`cmd/fuse`, Linux only)

- Semantics, flags, environment: **`docs/EXTERNAL_ACCESS.md`** §3.
- Same **`GODFS_TLS_*`** / **`GODFS_CLIENT_API_KEY`** as the CLI when the cluster uses TLS and keys.
- Unmount: `fusermount3 -u <mountpoint>` (or `umount`).
- Limits: no **truncate** / **`O_TRUNC`** in metadata (`EOPNOTSUPP`); **chmod/chown** → **`EPERM`**.

## Security

See `IMPLEMENTATION_PLAN.md` for the security feature set: TLS, API keys, JWKS, RBAC, audit (`GODFS_AUDIT_*`, `GODFS_AUDIT_CHUNK`).

Optional: **unary gRPC rate limit** — `GODFS_GRPC_RATE_LIMIT_RPS` / `GODFS_GRPC_RATE_LIMIT_BURST` (see `docs/SECURITY_COOKBOOK.md`); on Master **`RegisterNode`** and **`Heartbeat`** are not rate-limited.

## RBAC roles cookbook

Example `GODFS_RBAC_JSON` separating operator/admin and end-user (least privilege):

```json
[
  {
    "principal": "operator",
    "path_prefix": "/",
    "permissions": ["admin", "node", "read", "write", "delete"]
  },
  {
    "principal": "alice",
    "path_prefix": "/data",
    "permissions": ["read", "write", "delete"]
  },
  {
    "principal": "readonly",
    "path_prefix": "/data",
    "permissions": ["read"]
  }
]
```

Recommendations:

- **Operator** should use a dedicated token/API key with `admin` (snapshots/membership) + `node` (cluster RPCs).
- **Cluster principal** (`GODFS_CLUSTER_KEY`) is for internal services/nodes (chunk/master). Grant only what is required.
- For end-users avoid `admin`; scope with path prefixes.

## Secret rotation

### TLS (mTLS) without restart

When `GODFS_TLS_RELOAD=1`, processes periodically re-read certificate files:

- server (Master/Chunk gRPC): `GODFS_TLS_CERT_FILE`, `GODFS_TLS_KEY_FILE`, `GODFS_TLS_CA_FILE`
- REST gateway **HTTPS** listener (when `GODFS_REST_HTTPS_ENABLED`): `GODFS_REST_TLS_CERT_FILE` / `GODFS_REST_TLS_KEY_FILE` / `GODFS_REST_TLS_CA_FILE` (or cert/key fallback to `GODFS_TLS_*` — see `docs/EXTERNAL_ACCESS.md`)
- client mTLS: `GODFS_TLS_CLIENT_CERT_FILE`, `GODFS_TLS_CLIENT_KEY_FILE`

Procedure:

- update the secret in your secret manager → External Secrets Operator updates the Kubernetes Secret → file in the pod updates
- wait `GODFS_TLS_RELOAD_INTERVAL` (default 5s)
- verify new connections succeed (no TLS errors in logs)

Limitations:

- client-side `RootCAs` rotation still requires a restart (baseline). Server-side `ClientCAs` supports hot reload.

## Quality and CI

- CI: `.github/workflows/ci.yml` — **`lint`** (`golangci-lint`, including **copyloopvar**), **`test`**, **`rest-compose`** (compose + **`docker-compose.toxiproxy.yml`**: direct smoke, **`scripts/toxiproxy_rest_gate.sh`**, integration, chaos; for chaos set **`GODFS_DOCKER_COMPOSE_EXTRA`** as in CI).
- Extra gates: **`.github/workflows/blackbox.yml`** — long path **`scripts/blackbox_compose_gate.sh`**; **`.github/workflows/bench.yml`** — **`scripts/benchstat_report.sh`** and baseline under **`testdata/bench/`**.
- ChunkServer: on-disk chunk IDs are validated (`FSStore`); invalid id → gRPC **InvalidArgument** (`internal/adapter/repository/chunk/fs_store.go`).
- Snapshot **label:** `usecase.ValidateSnapshotLabel` / **`domain.ErrInvalidSnapshotLabel`** — REST **400**, gRPC **InvalidArgument**.
- Large file E2E: `GODFS_E2E_LARGE_BYTES` in `test/e2e/large_file_test.go` (default 8 MiB unless `-short`).

## Performance tuning

- **`GODFS_CLIENT_WRITE_PARALLELISM`** — max concurrent “chunk writes” in the SDK (integer ≥ 1; upper bound 64 in code).
- **`GODFS_CHUNK_READ_CACHE_ENTRIES`** — if set (>0), enable LRU ReadChunk response cache on ChunkServer.
- **`GODFS_CHUNK_READ_CACHE_MAX_BYTES`** — max size of one cached read slice (default 8 MiB when cache is on).
- **`GODFS_GRPC_MAX_MSG_BYTES`** — max gRPC message size (recv/send) on Master, ChunkServer, and client dial options (default 80 MiB). Must be **at least** the cluster chunk size (often 64 MiB).
- **`GODFS_SYNC_CHUNK_PART_BYTES`** — payload part size in the client `SyncChunk` stream for primary → secondary replication (default 256 KiB; minimum 4096 bytes in code).
- **`GODFS_CHUNK_READ_FRAME_BYTES`** — read buffer / frame size when serving `ReadChunk` to clients (default 32 KiB; minimum 1024 bytes).
- **E2E bench:** `go test ./test/e2e -bench BenchmarkE2E -benchtime 3x -run '^$'` (without `-short`).
