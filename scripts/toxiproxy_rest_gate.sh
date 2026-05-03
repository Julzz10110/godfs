#!/usr/bin/env bash
# Register a Toxiproxy front (host:18080) -> rest:8080 with latency toxics, then run REST smoke through the proxy.
# Requires Toxiproxy API on GODFS_TOXIPROXY_API (default http://127.0.0.1:8474) and proxy listening on host 18080.
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

# Prefer rest container IPv4 as upstream (avoids toxiproxy/musl DNS preferring IPv6 when REST listens on IPv4 only).
REST_CID="$("${COMPOSE[@]}" ps -q rest)"
if [[ -z "${REST_CID}" ]]; then
  echo "toxiproxy_rest_gate: no rest container id" >&2
  exit 1
fi
REST_RAW="$(docker inspect -f '{{range $n, $conf := .NetworkSettings.Networks}}{{$conf.IPAddress}} {{end}}' "${REST_CID}")"
REST_IP=""
for tok in ${REST_RAW}; do
  if [[ "$tok" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    REST_IP="$tok"
    break
  fi
done
UPSTREAM="rest:8080"
if [[ -n "${REST_IP}" ]]; then
  UPSTREAM="${REST_IP}:8080"
else
  echo "toxiproxy_rest_gate: could not resolve rest container IPv4, using hostname rest:8080" >&2
fi

curl -sS -X DELETE "${API}/proxies/rest" >/dev/null 2>&1 || true
curl -sf -X POST "${API}/proxies" \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"rest\",\"listen\":\"0.0.0.0:18080\",\"upstream\":\"${UPSTREAM}\"}"

# Moderate latency (enough to stress paths without flaking slow CI runners).
for stream in upstream downstream; do
  curl -sf -X POST "${API}/proxies/rest/toxics" \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"lat_${stream}\",\"type\":\"latency\",\"stream\":\"${stream}\",\"toxicity\":1,\"attributes\":{\"latency\":40,\"jitter\":5}}"
done

sleep 1

export REST_BASE_URL="${REST_BASE_URL:-http://127.0.0.1:18080}"
echo "Running REST smoke through Toxiproxy (${REST_BASE_URL}, upstream=${UPSTREAM}) ..."
bash "${ROOT}/scripts/rest_compose_smoke.sh"
echo "toxiproxy REST gate OK"
