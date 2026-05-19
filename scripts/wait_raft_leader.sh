#!/usr/bin/env bash
# Wait until a Raft leader is visible (gRPC masters list and/or metrics); dump compose logs on failure.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck source=raft_compose_lib.sh
source "${ROOT}/scripts/raft_compose_lib.sh"

COMPOSE_FILE="${GODFS_RAFT_COMPOSE_FILE:-deployments/docker/docker-compose.raft.yml}"
COMPOSE=(docker compose -f "$COMPOSE_FILE")
TIMEOUT_SEC="${GODFS_RAFT_LEADER_WAIT_TIMEOUT:-180}"
export GODFS_CLIENT_BIN="${GODFS_CLIENT_BIN:-${ROOT}/bin/godfs-client}"

declare -a METRICS_PORTS=(9091 9094 9096)
declare -a GRPC_PORTS=(9090 9093 9095)

grpc_has_leader() {
  raft_find_leader_index >/dev/null
}

deadline=$((SECONDS + TIMEOUT_SEC))
while ((SECONDS < deadline)); do
  if idx="$(raft_find_leader_index 2>/dev/null)"; then
    echo "Raft leader on master index ${idx} (gRPC 127.0.0.1:${GRPC_PORTS[$idx]})"
    exit 0
  fi
  sleep 2
done

echo "no Raft leader within ${TIMEOUT_SEC}s; compose logs:" >&2
"${COMPOSE[@]}" logs --no-color --tail=80 master-0 master-1 master-2 >&2 || true
exit 1
