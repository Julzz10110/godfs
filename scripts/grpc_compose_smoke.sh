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
CHUNK_WAIT_SEC="${GODFS_CHUNK_WAIT_SEC:-120}"
TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT

if ! wait_chunk_alive "$MASTER" "$CHUNK_WAIT_SEC"; then
  echo "no alive chunk node on leader ${MASTER} within ${CHUNK_WAIT_SEC}s" >&2
  exit 1
fi

_grpc_smoke_client() {
  local host_master="$1"
  shift
  if [[ "${GODFS_GRPC_SMOKE_USE_COMPOSE:-0}" == "1" ]]; then
    local compose_master
    compose_master="$(host_master_to_compose_master "$host_master")" || return 1
    godfs_client_compose "$compose_master" "$@"
  else
    godfs_client --master "$host_master" "$@"
  fi
}

_grpc_smoke_client "$MASTER" mkdir "$PREFIX"
_grpc_smoke_client "$MASTER" create "${PREFIX}/ok.txt"
echo -n 'grpc-smoke-ok' >"$TMP"
_grpc_smoke_client "$MASTER" write "${PREFIX}/ok.txt" "$TMP"
_grpc_smoke_client "$MASTER" read "${PREFIX}/ok.txt" "${TMP}.out"
got="$(cat "${TMP}.out")"
if [[ "$got" != 'grpc-smoke-ok' ]]; then
  echo "read mismatch: got $got" >&2
  exit 1
fi
echo "gRPC compose smoke OK on $MASTER"
