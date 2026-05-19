#!/usr/bin/env bash
# kill Raft leader master container, wait for new leader, verify gRPC read/write.
# GODFS_RAFT_CHAOS_QUORUM_BREAK=1 kills two masters and expects failure (no quorum).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck source=raft_compose_lib.sh
source "${ROOT}/scripts/raft_compose_lib.sh"

COMPOSE_FILE="${GODFS_RAFT_COMPOSE_FILE:-deployments/docker/docker-compose.raft.yml}"
COMPOSE=(docker compose -f "$COMPOSE_FILE")
TIMEOUT_SEC="${GODFS_RAFT_LEADER_CHAOS_TIMEOUT:-30}"
QUORUM_BREAK="${GODFS_RAFT_CHAOS_QUORUM_BREAK:-0}"
export GODFS_CLIENT_BIN="${GODFS_CLIENT_BIN:-${ROOT}/bin/godfs-client}"

# host metrics_port -> host grpc_port
declare -a METRICS_PORTS=(9091 9094 9096)
declare -a GRPC_PORTS=(9090 9093 9095)
declare -a SERVICES=(master-0 master-1 master-2)

wait_for_leader() {
  GODFS_RAFT_LEADER_WAIT_TIMEOUT="$TIMEOUT_SEC" bash scripts/wait_raft_leader.sh
}

leader_grpc_addr() {
  local idx="$1"
  echo "127.0.0.1:${GRPC_PORTS[$idx]}"
}

if [[ "$QUORUM_BREAK" == "1" ]]; then
  echo "Raft chaos: quorum break test (kill 2 of 3 masters)"
  for svc in master-0 master-1; do
    cid="$("${COMPOSE[@]}" ps -q "$svc")"
    [[ -n "$cid" ]] || { echo "no container for $svc" >&2; exit 1; }
    docker kill -s KILL "$cid" || true
  done
  sleep 5
  if raft_find_leader_index >/dev/null 2>&1; then
    echo "unexpected leader after quorum loss" >&2
    exit 1
  fi
  echo "quorum break: no leader as expected"
  exit 0
fi

echo "Raft chaos: waiting for initial leader..."
wait_for_leader
idx="$(raft_find_leader_index)" || {
  echo "leader not found on any master gRPC/metrics port" >&2
  exit 1
}
leader_svc="${SERVICES[$idx]}"
echo "leader is ${leader_svc} (grpc $(leader_grpc_addr "$idx"))"

export GODFS_MASTER_ADDR="$(leader_grpc_addr "$idx")"
export GRPC_SMOKE_PREFIX="${GRPC_SMOKE_PREFIX:-/smoke_raft_chaos}"
bash scripts/grpc_compose_smoke.sh

echo "Raft chaos: SIGKILL leader ${leader_svc}"
cid="$("${COMPOSE[@]}" ps -q "$leader_svc")"
[[ -n "$cid" ]] || { echo "no container for ${leader_svc}" >&2; exit 1; }
docker kill -s KILL "$cid" || true

echo "Raft chaos: waiting for new leader..."
wait_for_leader
new_idx="$(raft_find_leader_index)" || {
  echo "new leader not found after kill" >&2
  exit 1
}
if [[ "$new_idx" == "$idx" ]]; then
  echo "leader index unchanged after kill (still ${leader_svc})" >&2
  exit 1
fi
new_svc="${SERVICES[$new_idx]}"
echo "new leader is ${new_svc} (grpc $(leader_grpc_addr "$new_idx"))"

export GODFS_MASTER_ADDR="$(leader_grpc_addr "$new_idx")"
export GRPC_SMOKE_PREFIX="${GRPC_SMOKE_PREFIX}_after"
bash scripts/grpc_compose_smoke.sh

echo "compose raft leader chaos OK"
