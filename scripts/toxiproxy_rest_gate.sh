#!/usr/bin/env bash
# Register a Toxiproxy front (host:18080) -> rest:8080 with latency toxics, then run REST smoke through the proxy.
# Requires Toxiproxy API on GODFS_TOXIPROXY_API (default http://127.0.0.1:8474) and proxy listening on host 18080.
set -euo pipefail

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

# ~75ms each direction + small jitter (REST smoke should still pass with default timeouts).
for stream in upstream downstream; do
  curl -sf -X POST "${API}/proxies/rest/toxics" \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"lat_${stream}\",\"type\":\"latency\",\"stream\":\"${stream}\",\"toxicity\":1,\"attributes\":{\"latency\":75,\"jitter\":10}}"
done

export REST_BASE_URL="${REST_BASE_URL:-http://127.0.0.1:18080}"
echo "Running REST smoke through Toxiproxy (${REST_BASE_URL}) ..."
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bash "${ROOT}/scripts/rest_compose_smoke.sh"
echo "toxiproxy REST gate OK"
