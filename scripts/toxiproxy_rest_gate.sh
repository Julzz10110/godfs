#!/usr/bin/env bash
# Register a Toxiproxy front (container :18080) -> rest:8080 with latency toxics, then run REST smoke
# through the proxy from a throwaway container on the same Compose network (avoids host IPv4 vs [::]
# publish quirks on some Linux runners).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COMPOSE=(docker compose -f deployments/docker/docker-compose.yml)
if [[ -n "${GODFS_DOCKER_COMPOSE_EXTRA:-}" ]]; then
  COMPOSE+=(-f "${GODFS_DOCKER_COMPOSE_EXTRA}")
fi

API="${GODFS_TOXIPROXY_API:-http://127.0.0.1:8474}"

echo "Waiting for Toxiproxy API at ${API} ..."
for i in $(seq 1 60); do
  if curl -sf "${API}/proxies" >/dev/null; then
    break
  fi
  sleep 1
  if [[ "$i" -eq 60 ]]; then
    echo "timeout waiting for toxiproxy" >&2
    exit 1
  fi
done

curl -sS -X DELETE "${API}/proxies/rest" >/dev/null 2>&1 || true
curl -sf -X POST "${API}/proxies" \
  -H 'Content-Type: application/json' \
  -d '{"name":"rest","listen":"0.0.0.0:18080","upstream":"rest:8080"}'

for stream in upstream downstream; do
  curl -sf -X POST "${API}/proxies/rest/toxics" \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"lat_${stream}\",\"type\":\"latency\",\"stream\":\"${stream}\",\"toxicity\":1,\"attributes\":{\"latency\":40,\"jitter\":5}}"
done

sleep 1

TOXI_CID="$("${COMPOSE[@]}" ps -q toxiproxy)"
if [[ -z "${TOXI_CID}" ]]; then
  echo "toxiproxy_rest_gate: no toxiproxy container" >&2
  exit 1
fi
NET="$(docker inspect -f '{{range $k, $_ := .NetworkSettings.Networks}}{{$k}} {{end}}' "${TOXI_CID}" | awk '{print $1; exit}')"
if [[ -z "${NET}" ]]; then
  echo "toxiproxy_rest_gate: could not read compose network from toxiproxy container" >&2
  exit 1
fi

echo "Running REST smoke on Docker network=${NET} via http://toxiproxy:18080 ..."
docker run --rm \
  --network "${NET}" \
  -e REST_BASE_URL=http://toxiproxy:18080 \
  -e GODFS_TEST_API_KEY="${GODFS_TEST_API_KEY:-}" \
  -v "${ROOT}:/work:ro" \
  alpine:3.20 \
  sh -c 'apk add --no-cache bash curl python3 >/dev/null && exec bash /work/scripts/rest_compose_smoke.sh'

echo "toxiproxy REST gate OK"
