#!/usr/bin/env bash
# gRPC smoke via godfs-client (used after Raft leader failover when REST may still target a follower).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck source=raft_compose_lib.sh
source "${ROOT}/scripts/raft_compose_lib.sh"

export GODFS_CLIENT_BIN="${GODFS_CLIENT_BIN:-${ROOT}/bin/godfs-client}"

MASTER="${GODFS_MASTER_ADDR:?set GODFS_MASTER_ADDR (host:port)}"
PREFIX="${GRPC_SMOKE_PREFIX:-/smoke_grpc}"
TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT

godfs_client --master "$MASTER" mkdir "$PREFIX"
godfs_client --master "$MASTER" create "${PREFIX}/ok.txt"
echo -n 'grpc-smoke-ok' >"$TMP"
godfs_client --master "$MASTER" write "${PREFIX}/ok.txt" "$TMP"
godfs_client --master "$MASTER" read "${PREFIX}/ok.txt" "${TMP}.out"
got="$(cat "${TMP}.out")"
if [[ "$got" != 'grpc-smoke-ok' ]]; then
  echo "read mismatch: got $got" >&2
  exit 1
fi
echo "gRPC compose smoke OK on $MASTER"
