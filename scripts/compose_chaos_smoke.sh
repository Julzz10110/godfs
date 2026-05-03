#!/usr/bin/env bash
# Chaos smoke: kill ChunkServer container, bring it back, verify REST path still works.
# Note: this compose profile uses a single in-memory master (no Raft); leader-kill
# metadata chaos is covered by in-process e2e (see TestE2E_RaftMaster_ReplicationAndFailover in CI).
# Prerequisites: docker compose stack already up (same file as rest-compose job).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
COMPOSE=(docker compose -f deployments/docker/docker-compose.yml)
if [[ -n "${GODFS_DOCKER_COMPOSE_EXTRA:-}" ]]; then
  COMPOSE+=(-f "${GODFS_DOCKER_COMPOSE_EXTRA}")
fi
export REST_BASE_URL="${REST_BASE_URL:-http://127.0.0.1:8080}"

echo "Chaos: SIGKILL chunk"
CID="$("${COMPOSE[@]}" ps -q chunk)"
if [[ -z "${CID}" ]]; then
  echo "no chunk container id" >&2
  exit 1
fi
docker kill -s KILL "${CID}" || true
sleep 3
"${COMPOSE[@]}" up -d chunk

echo "Waiting for REST health after chunk restart..."
for i in $(seq 1 90); do
  if curl -sf "${REST_BASE_URL}/v1/health" >/dev/null; then
    break
  fi
  sleep 1
  if [[ "$i" -eq 90 ]]; then
    echo "timeout waiting for /v1/health" >&2
    exit 1
  fi
done

bash scripts/rest_compose_smoke.sh
echo "compose chaos smoke OK"
