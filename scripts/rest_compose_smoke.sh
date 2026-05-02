#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${REST_BASE_URL:-http://127.0.0.1:8080}"

echo "Waiting for REST health at $BASE_URL ..."
for i in $(seq 1 90); do
  if curl -sf "$BASE_URL/v1/health" >/dev/null; then
    break
  fi
  sleep 1
  if [[ "$i" -eq 90 ]]; then
    echo "timeout waiting for /v1/health" >&2
    exit 1
  fi
done

AUTH=()
if [[ -n "${GODFS_TEST_API_KEY:-}" ]]; then
  AUTH=(-H "Authorization: Bearer ${GODFS_TEST_API_KEY}")
fi

curl -sf "${AUTH[@]}" -X POST "$BASE_URL/v1/fs/mkdir" \
  -H 'Content-Type: application/json' \
  -d '{"path":"/smoke"}'

curl -sf "${AUTH[@]}" -X POST "$BASE_URL/v1/fs/file" \
  -H 'Content-Type: application/json' \
  -d '{"path":"/smoke/hello.txt"}'

echo -n 'ci-smoke-hello' | curl -sf "${AUTH[@]}" -X PUT "$BASE_URL/v1/fs/content?path=/smoke/hello.txt" --data-binary @-

got="$(curl -sf "${AUTH[@]}" "$BASE_URL/v1/fs/content?path=/smoke/hello.txt")"
if [[ "$got" != 'ci-smoke-hello' ]]; then
  echo "GET content mismatch: got len=${#got}" >&2
  exit 1
fi

# S3-style multipart upload (two parts)
up="$(curl -sf "${AUTH[@]}" -X POST "$BASE_URL/v1/fs/multipart" \
  -H 'Content-Type: application/json' \
  -d '{"path":"/smoke/mp.bin"}')"
uid="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["upload_id"])' "$up")"

echo -n 'aa' | curl -sf "${AUTH[@]}" -X PUT "$BASE_URL/v1/fs/multipart/${uid}?partNumber=1" --data-binary @-
echo -n 'bb' | curl -sf "${AUTH[@]}" -X PUT "$BASE_URL/v1/fs/multipart/${uid}?partNumber=2" --data-binary @-

curl -sf "${AUTH[@]}" -X POST "$BASE_URL/v1/fs/multipart/${uid}/complete" \
  -H 'Content-Type: application/json' \
  -d '{"parts":[{"part_number":1},{"part_number":2}]}' >/dev/null

mpgot="$(curl -sf "${AUTH[@]}" "$BASE_URL/v1/fs/content?path=/smoke/mp.bin")"
if [[ "$mpgot" != 'aabb' ]]; then
  echo "multipart GET mismatch" >&2
  exit 1
fi

echo "REST compose smoke OK"
