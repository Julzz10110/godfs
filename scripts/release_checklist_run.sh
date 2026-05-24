#!/usr/bin/env bash
# Run release checklist R1–R10 (see docs/RELEASE_CHECKLIST.md).
# Requires: go, bash, curl, docker (for R1–R3, R2, R8). Linux recommended for R9 FUSE tests.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck source=raft_compose_lib.sh
source "${ROOT}/scripts/raft_compose_lib.sh"

COMPOSE_BASE="${GODFS_RAFT_COMPOSE_FILE:-deployments/docker/docker-compose.raft.yml}"
COMPOSE_OVERLAY="deployments/docker/docker-compose.release-checklist.yml"
COMPOSE=(docker compose -f "$COMPOSE_BASE" -f "$COMPOSE_OVERLAY")
export GODFS_CLIENT_BIN="${GODFS_CLIENT_BIN:-${ROOT}/bin/godfs-client}"
export GODFS_COMPOSE_EXTRA_FILE="$COMPOSE_OVERLAY"

R1_MB="${RELEASE_R1_SIZE_MB:-100}"
R2_HEAL_SEC="${RELEASE_R2_HEAL_SEC:-900}"
RESULT_FILE="${RELEASE_CHECKLIST_RESULT:-${ROOT}/release_checklist_result.txt}"

pass() { echo "PASS $*" | tee -a "$RESULT_FILE"; }
fail() { echo "FAIL $*" | tee -a "$RESULT_FILE" >&2; exit 1; }
skip() { echo "SKIP $*" | tee -a "$RESULT_FILE"; }
section() { echo ""; echo "======== $* ========" | tee -a "$RESULT_FILE"; }

: >"$RESULT_FILE"
echo "release_checklist_run started $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$RESULT_FILE"

section "R4 presigned GET/PUT (unit + e2e)"
go test ./internal/restgateway/... -run 'Presign' -count=1
go test ./test/e2e -run 'PresignedPUT' -count=1 -timeout=5m
pass "R4 presign unit + e2e"

section "R10 API key reload (unit)"
go test ./internal/security/... -run 'AuthReload|LoadAuthFromEnv' -count=1
pass "R10 auth reload unit"

section "R5 RBAC + TLS config (unit)"
go test ./internal/security/... -run 'RBAC|PermFromMethod|HTTPServerTLS' -count=1
pass "R5 RBAC/TLS unit (full mTLS deny/allow: staging with GODFS_RBAC_JSON + client certs)"

section "R6 observability"
bash scripts/observability_check.sh
pass "R6 observability_check"

section "R7 snapshot (e2e)"
go test ./test/e2e -run 'BackupSnapshot' -count=1 -timeout=5m
pass "R7 snapshot e2e (DR walkthrough: deployments/k8s/dr/README.md)"

section "R9 FUSE (Linux unit)"
if [[ "$(go env GOOS)" == "linux" ]]; then
	go test ./cmd/fuse/... -count=1 -timeout=3m
	pass "R9 FUSE tests"
else
	skip "R9 FUSE tests (requires GOOS=linux, run in CI test job)"
fi

if ! command -v docker >/dev/null 2>&1; then
	skip "R1 R2 R3 R8 (docker not available)"
	echo "Done (partial). See $RESULT_FILE"
	exit 0
fi

section "R1–R3 R8 docker compose"
go build -o bin/godfs-client ./cmd/client

"${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
"${COMPOSE[@]}" up -d --build master-0 master-1 master-2
bash scripts/raft_compose_bootstrap.sh

echo "Starting chunk servers and REST (masters must not be recreated) ..."
"${COMPOSE[@]}" up -d --no-recreate chunk chunk-b rest

echo "Waiting for Raft leader ..."
GODFS_RAFT_LEADER_WAIT_TIMEOUT="${GODFS_RAFT_LEADER_WAIT_TIMEOUT:-180}" \
	bash scripts/wait_raft_leader.sh

GODFS_MASTER_ADDR="$(raft_leader_grpc_addr)" || fail "no Raft leader for client"
export GODFS_MASTER_ADDR
echo "Using master gRPC ${GODFS_MASTER_ADDR}"

echo "Waiting for two chunk nodes (replication=2) ..."
wait_chunks_alive_min "$GODFS_MASTER_ADDR" 2 "${GODFS_CHUNK_WAIT_SEC:-180}" \
	|| fail "expected 2 alive chunk nodes"
echo "Waiting for chunk heartbeats (${RELEASE_CHUNK_HB_WAIT_SEC:-8}s) ..."
sleep "${RELEASE_CHUNK_HB_WAIT_SEC:-8}"

section "R8 operator CLI"
godfs_client --master "$GODFS_MASTER_ADDR" nodes
godfs_client --master "$GODFS_MASTER_ADDR" rebalance-run --steps 3
if ! godfs_client --master "$GODFS_MASTER_ADDR" chunks under-replicated; then
	fail "R8 chunks under-replicated check failed (gRPC error)"
fi
pass "R8 nodes rebalance under-replicated"

section "R1 large upload SHA-256"
PREFIX="/release_r1"
TMPDIR="${TMPDIR:-/tmp}"
IN="${TMPDIR}/godfs_r1_in.bin"
OUT="${TMPDIR}/godfs_r1_out.bin"
dd if=/dev/urandom of="$IN" bs=1M count="$R1_MB" status=none 2>/dev/null \
	|| dd if=/dev/urandom of="$IN" bs=1048576 count="$R1_MB" status=none
HASH_IN="$(sha256sum "$IN" | awk '{print $1}')"
COMPOSE_MASTER="$(compose_master_grpc_addr)" || fail "no Raft leader for compose client"
godfs_client_compose "$COMPOSE_MASTER" mkdir "$PREFIX" || true
godfs_client_compose "$COMPOSE_MASTER" create "${PREFIX}/big.bin" || true
godfs_client_compose "$COMPOSE_MASTER" write "${PREFIX}/big.bin" "$IN"
godfs_client_compose "$COMPOSE_MASTER" read "${PREFIX}/big.bin" "$OUT"
HASH_OUT="$(sha256sum "$OUT" | awk '{print $1}')"
rm -f "$IN" "$OUT"
if [[ "$HASH_IN" != "$HASH_OUT" ]]; then
	fail "R1 checksum mismatch"
fi
pass "R1 ${R1_MB}MiB upload/read SHA-256"

section "R2 under-replicated healing"
# ListUnderReplicated is empty when replication<=1; overlay uses RF=2 and two chunks.
# Auto-rebalance is disabled in compose overlay; poll after NODE_DEAD_AFTER.
"${COMPOSE[@]}" stop chunk
r2_deadline=$((SECONDS + "${RELEASE_R2_DETECT_SEC:-45}"))
urc=0
while ((SECONDS < r2_deadline)); do
	sleep 2
	GODFS_MASTER_ADDR="$(raft_leader_grpc_addr)" || true
	export GODFS_MASTER_ADDR
	set +e
	godfs_client --master "$GODFS_MASTER_ADDR" chunks under-replicated
	urc=$?
	set -e
	if [[ "$urc" -eq 1 ]]; then
		break
	fi
done
if [[ "$urc" -ne 1 ]]; then
	fail "R2 expected under-replicated within ${RELEASE_R2_DETECT_SEC:-45}s after stopping chunk"
fi
"${COMPOSE[@]}" start chunk
GODFS_MASTER_ADDR="$(raft_leader_grpc_addr)" || fail "no Raft leader during R2 heal"
export GODFS_MASTER_ADDR
wait_chunks_alive_min "$GODFS_MASTER_ADDR" 2 "${GODFS_CHUNK_WAIT_SEC:-180}"
deadline=$((SECONDS + R2_HEAL_SEC))
healed=0
while ((SECONDS < deadline)); do
	godfs_client --master "$GODFS_MASTER_ADDR" rebalance-run --steps 5 || true
	if godfs_client --master "$GODFS_MASTER_ADDR" chunks under-replicated; then
		healed=1
		break
	fi
	sleep 5
done
if [[ "$healed" -ne 1 ]]; then
	fail "R2 healing timeout ${R2_HEAL_SEC}s"
fi
pass "R2 under-replicated then healed"

section "R3 Raft leader failover"
export GODFS_RAFT_COMPOSE_FILE="$COMPOSE_BASE"
export GODFS_GRPC_SMOKE_USE_COMPOSE=1
bash scripts/compose_raft_leader_chaos.sh
pass "R3 raft leader chaos"

"${COMPOSE[@]}" down -v >/dev/null 2>&1 || true

section "DONE"
echo "All release checklist steps completed. See $RESULT_FILE"
