#!/usr/bin/env bash
# ~200ms RTT via tc netem on chunk, or toxiproxy fallback when tc is not permitted.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COMPOSE=(docker compose -f deployments/docker/docker-compose.yml)
if [[ -n "${GODFS_DOCKER_COMPOSE_EXTRA:-}" ]]; then
  COMPOSE+=(-f "${GODFS_DOCKER_COMPOSE_EXTRA}")
fi
if [[ -f deployments/docker/docker-compose.netem.yml ]]; then
  COMPOSE+=(-f deployments/docker/docker-compose.netem.yml)
fi

NETEM_DELAY_MS="${GODFS_NETEM_DELAY_MS:-200}"
NETEM_JITTER_MS="${GODFS_NETEM_JITTER_MS:-20}"
IFACE="${GODFS_NETEM_IFACE:-eth0}"

apply_toxiproxy_latency() {
  local latency="${1:?}"
  local jitter="${2:?}"
  local API="${GODFS_TOXIPROXY_API:-http://127.0.0.1:8474}"
  echo "netem fallback: toxiproxy latency ${latency}ms jitter ${jitter}ms on rest upstream/downstream"
  for i in $(seq 1 60); do
    if curl -sf "${API}/proxies" >/dev/null; then
      break
    fi
    sleep 1
    [[ "$i" -eq 60 ]] && { echo "toxiproxy API unavailable" >&2; return 1; }
  done
  curl -sS -X DELETE "${API}/proxies/rest_netem" >/dev/null 2>&1 || true
  curl -sf -X POST "${API}/proxies" \
    -H 'Content-Type: application/json' \
    -d '{"name":"rest_netem","listen":"0.0.0.0:18081","upstream":"rest:8080"}'
  for stream in upstream downstream; do
    curl -sf -X POST "${API}/proxies/rest_netem/toxics" \
      -H 'Content-Type: application/json' \
      -d "{\"name\":\"netem_${stream}\",\"type\":\"latency\",\"stream\":\"${stream}\",\"toxicity\":1,\"attributes\":{\"latency\":${latency},\"jitter\":${jitter}}}"
  done
  TOXI_CID="$("${COMPOSE[@]}" ps -q toxiproxy)"
  [[ -n "${TOXI_CID}" ]] || { echo "no toxiproxy container" >&2; return 1; }
  docker run --rm \
    --network "container:${TOXI_CID}" \
    -e REST_BASE_URL=http://127.0.0.1:18081 \
    -e REST_SMOKE_PREFIX="${REST_SMOKE_PREFIX:-/smoke_netem}" \
    -e GODFS_TEST_API_KEY="${GODFS_TEST_API_KEY:-}" \
    -v "${ROOT}:/work:ro" \
    alpine:3.20 \
    sh -c 'apk add --no-cache bash curl python3 >/dev/null && exec bash /work/scripts/rest_compose_smoke.sh'
}

CHUNK_CID="$("${COMPOSE[@]}" ps -q chunk)"
if [[ -z "${CHUNK_CID}" ]]; then
  echo "no chunk container" >&2
  exit 1
fi

cleanup() {
  docker exec "$CHUNK_CID" sh -c "tc qdisc del dev ${IFACE} root 2>/dev/null || true" 2>/dev/null || true
}
trap cleanup EXIT

echo "netem: trying ${NETEM_DELAY_MS}ms delay on chunk ${IFACE}"
set +e
TC_OUT="$(docker exec "$CHUNK_CID" sh -c "
  command -v tc >/dev/null || apk add --no-cache iproute2 >/dev/null 2>&1
  tc qdisc replace dev ${IFACE} root netem delay ${NETEM_DELAY_MS}ms ${NETEM_JITTER_MS}ms
" 2>&1)"
TC_RC=$?
set -e

if [[ "$TC_RC" -ne 0 ]]; then
  echo "tc netem failed (${TC_OUT}); using toxiproxy fallback" >&2
  apply_toxiproxy_latency "$NETEM_DELAY_MS" "$NETEM_JITTER_MS"
  echo "compose netem gate OK (toxiproxy fallback)"
  exit 0
fi

export REST_SMOKE_PREFIX="${REST_SMOKE_PREFIX:-/smoke_netem}"
timeout 600 bash scripts/rest_compose_smoke.sh
echo "compose netem gate OK (tc)"
