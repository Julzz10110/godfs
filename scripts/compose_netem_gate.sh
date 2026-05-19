#!/usr/bin/env bash
# apply ~200ms RTT on chunk egress via tc netem; REST smoke must still complete (no hang).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COMPOSE=(docker compose -f deployments/docker/docker-compose.yml)
if [[ -n "${GODFS_DOCKER_COMPOSE_EXTRA:-}" ]]; then
  COMPOSE+=(-f "${GODFS_DOCKER_COMPOSE_EXTRA}")
fi

CHUNK_CID="$("${COMPOSE[@]}" ps -q chunk)"
if [[ -z "${CHUNK_CID}" ]]; then
  echo "no chunk container" >&2
  exit 1
fi

NETEM_DELAY_MS="${GODFS_NETEM_DELAY_MS:-200}"
NETEM_JITTER_MS="${GODFS_NETEM_JITTER_MS:-20}"
IFACE="${GODFS_NETEM_IFACE:-eth0}"

cleanup() {
  docker exec "$CHUNK_CID" sh -c "tc qdisc del dev ${IFACE} root 2>/dev/null || true" || true
}
trap cleanup EXIT

echo "netem: ${NETEM_DELAY_MS}ms delay on chunk ${IFACE}"
docker exec "$CHUNK_CID" sh -c "
  command -v tc >/dev/null || apk add --no-cache iproute2 >/dev/null
  tc qdisc replace dev ${IFACE} root netem delay ${NETEM_DELAY_MS}ms ${NETEM_JITTER_MS}ms
"

export REST_SMOKE_PREFIX="${REST_SMOKE_PREFIX:-/smoke_netem}"
timeout 600 bash scripts/rest_compose_smoke.sh
echo "compose netem gate OK"
