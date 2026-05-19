#!/usr/bin/env bash
# Wait until a Raft leader is visible (gRPC masters list and/or metrics); dump compose logs on failure.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COMPOSE_FILE="${GODFS_RAFT_COMPOSE_FILE:-deployments/docker/docker-compose.raft.yml}"
COMPOSE=(docker compose -f "$COMPOSE_FILE")
TIMEOUT_SEC="${GODFS_RAFT_LEADER_WAIT_TIMEOUT:-180}"
MASTER_GRPC="${GODFS_RAFT_BOOTSTRAP_MASTER:-127.0.0.1:9090}"
declare -a METRICS_PORTS=(9091 9094 9096)

# curl | grep -q (not echo | grep) — with pipefail, echo closes early and yields SIGPIPE / false failure.
metrics_is_leader() {
  local port="$1"
  curl -sf "http://127.0.0.1:${port}/metrics" 2>/dev/null \
    | grep -qE '^godfs_raft_is_leader (1|1\.0*)( |$)'
}

grpc_has_leader() {
  local out leader
  out=$(go run ./cmd/client --master "$MASTER_GRPC" masters list 2>/dev/null) || return 1
  leader=$(grep -m1 '^leader_node_id=' <<<"$out" | cut -d= -f2- | tr -d '\r\n')
  [[ -n "$leader" ]]
}

deadline=$((SECONDS + TIMEOUT_SEC))
while ((SECONDS < deadline)); do
  if grpc_has_leader; then
    echo "Raft leader via gRPC (${MASTER_GRPC})"
    exit 0
  fi
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
