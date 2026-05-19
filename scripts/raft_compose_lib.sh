# Shared helpers for Raft docker-compose scripts (source from bash, do not execute directly).
: "${ROOT:?ROOT must be set}"

godfs_client() {
  if [[ -n "${GODFS_CLIENT_BIN:-}" && -x "${GODFS_CLIENT_BIN}" ]]; then
    "${GODFS_CLIENT_BIN}" "$@"
  elif [[ -x "${ROOT}/bin/godfs-client" ]]; then
    "${ROOT}/bin/godfs-client" "$@"
  else
    go run ./cmd/client "$@"
  fi
}

metrics_is_leader() {
  local port="$1"
  curl -sf "http://127.0.0.1:${port}/metrics" 2>/dev/null \
    | grep -qE '^godfs_raft_is_leader (1|1\.0*)( |$)'
}

# Echo 0..N-1 index of the master that answers masters list as leader; exit 1 if none.
# Requires GRPC_PORTS and METRICS_PORTS arrays in the caller.
raft_find_leader_index() {
  local i addr out leader
  for i in "${!GRPC_PORTS[@]}"; do
    addr="127.0.0.1:${GRPC_PORTS[$i]}"
    if out=$(godfs_client --master "$addr" masters list 2>/dev/null); then
      leader=$(grep -m1 '^leader_node_id=' <<<"$out" | cut -d= -f2- | tr -d '\r\n')
      if [[ -n "$leader" ]]; then
        echo "$i"
        return 0
      fi
    fi
  done
  for i in "${!METRICS_PORTS[@]}"; do
    if metrics_is_leader "${METRICS_PORTS[$i]}"; then
      echo "$i"
      return 0
    fi
  done
  return 1
}
