#!/usr/bin/env bash
# Long black-box gate: compose (+ optional toxiproxy overlay), REST smoke, toxiproxy path,
# integration tests, chunk chaos. Intended for workflow_dispatch / schedule (see .github/workflows/blackbox.yml).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

EXTRA="${GODFS_DOCKER_COMPOSE_EXTRA:-}"
if [[ -n "${EXTRA}" ]]; then
  COMPOSE=(docker compose -f deployments/docker/docker-compose.yml -f "${EXTRA}")
else
  COMPOSE=(docker compose -f deployments/docker/docker-compose.yml)
fi

echo "Blackbox: compose up (--build)"
"${COMPOSE[@]}" up -d --build

cleanup() {
  echo "Blackbox: compose down"
  "${COMPOSE[@]}" down -v || true
}
trap cleanup EXIT

export REST_BASE_URL="${REST_BASE_URL:-http://127.0.0.1:8080}"
bash scripts/rest_compose_smoke.sh

if [[ -n "${EXTRA}" ]]; then
  echo "Blackbox: toxiproxy REST path"
  bash scripts/toxiproxy_rest_gate.sh
fi

echo "Blackbox: integration tests (direct REST port)"
go test ./test/integration -tags=integration -count=1 -timeout=10m -v

export GODFS_DOCKER_COMPOSE_EXTRA="${EXTRA}"
bash scripts/compose_chaos_smoke.sh

echo "blackbox_compose_gate OK"
