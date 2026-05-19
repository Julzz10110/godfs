#!/usr/bin/env bash
# Wait until godfs_raft_is_leader=1 on any master metrics port; dump compose logs on failure.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COMPOSE_FILE="${GODFS_RAFT_COMPOSE_FILE:-deployments/docker/docker-compose.raft.yml}"
COMPOSE=(docker compose -f "$COMPOSE_FILE")
TIMEOUT_SEC="${GODFS_RAFT_LEADER_WAIT_TIMEOUT:-180}"
declare -a METRICS_PORTS=(9091 9094 9096)

metrics_is_leader() {
  local port="$1"
  curl -sf "http://127.0.0.1:${port}/metrics" 2>/dev/null | grep -qE '^godfs_raft_is_leader\s+1'
}

deadline=$((SECONDS + TIMEOUT_SEC))
while ((SECONDS < deadline)); do
  for p in "${METRICS_PORTS[@]}"; do
    if metrics_is_leader "$p"; then
      echo "Raft leader metrics on :${p}"
      exit 0
    fi
  done
  sleep 2
done

echo "no Raft leader within ${TIMEOUT_SEC}s; compose logs:" >&2
"${COMPOSE[@]}" logs --no-color --tail=80 master-0 master-1 master-2 >&2 || true
exit 1
