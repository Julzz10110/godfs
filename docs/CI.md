# CI and quality gates

## Workflows

| Job | Workflow | Purpose |
|-----|----------|---------|
| `lint` | `ci.yml` | golangci-lint (`errcheck`, `gofumpt`, …) |
| `test` | `ci.yml` | unit tests, e2e (incl. Raft failover), FUSE build, `bench_gate.sh` |
| `observability` | `ci.yml` | M4: promtool + Helm rules sync |
| `rest-compose` | `ci.yml` | Docker stack, REST smoke, toxiproxy, integration, chunk chaos, **netem** |
| `raft-compose` | `ci.yml` | 3× master Raft, bootstrap, leader-kill gRPC smoke, quorum-break |
| `bench` | `bench.yml` | weekly unit bench + **e2e bench artifact** |
| `testcontainers` | `ci.yml` | **testcontainers-go** compose up + `/v1/health` (set `GODFS_TESTCONTAINERS=1`, non-blocking) |

## Scripts

| Script | Description |
|--------|-------------|
| `scripts/compose_raft_leader_chaos.sh` | Kill Raft leader; gRPC smoke on new leader (`GODFS_RAFT_LEADER_CHAOS_TIMEOUT`, default 30s) |
| `scripts/compose_raft_leader_chaos.sh` + `GODFS_RAFT_CHAOS_QUORUM_BREAK=1` | Kill 2 masters; expect no leader |
| `scripts/compose_netem_gate.sh` | tc netem ~200ms on chunk; REST smoke |
| `scripts/bench_e2e_report.sh` | Write `BenchmarkE2E_*` output for bench workflow |

### Raft compose stack

CI order: **masters → bootstrap → chunk/rest → chaos** (chunk registers only after Raft leader exists).

Bootstrap is **single-node then AddMaster** (same as e2e), not a 3-voter cold start. Host-run `godfs-client` uses `GODFS_ADVERTISE_ADDR=127.0.0.1:8000` (published port).

```bash
go build -o bin/godfs-client ./cmd/client
docker compose -f deployments/docker/docker-compose.raft.yml up -d --build master-0 master-1 master-2
bash scripts/raft_compose_bootstrap.sh
docker compose -f deployments/docker/docker-compose.raft.yml up -d chunk rest
bash scripts/compose_raft_leader_chaos.sh
```

Netem uses `docker-compose.netem.yml` (`privileged: true` on chunk); if `tc` fails, `compose_netem_gate.sh` falls back to Toxiproxy latency.

### Update e2e bench baseline

1. Capture baseline:  
   `go test ./test/e2e -bench='^BenchmarkE2E_SingleChunkWrite_1Replica$' -benchtime=10x -count=6 -run='^$' > testdata/bench/baseline_BenchmarkE2E_SingleChunkWrite_1Replica.txt`
2. Compare locally: `bash scripts/benchstat_e2e_report.sh bench_E2E.txt`
3. Optional nightly gate: `bash scripts/bench_e2e_gate.sh` (also run in `bench.yml` via benchstat step).

Runtime **p95/p99** SLO remains in Prometheus (`deployments/observability/`); e2e bench guards throughput regressions.

### Failed replication / SyncChunk stall

If **SyncChunk** to a secondary fails or times out, **CommitChunk** is not reached (client sees error). A created file may exist with **size &lt; written bytes** until repair/GC; see `TestE2E_SyncChunkStallOnReplicate` and `docs/DATA_PLANE.md`.

## Local integration tests

```bash
docker compose -f deployments/docker/docker-compose.yml up -d --build
go test ./test/integration -tags=integration -count=1 -v
```

Includes partial PUT (`TestREST_PartialPutDoesNotCommitFullObject`).
