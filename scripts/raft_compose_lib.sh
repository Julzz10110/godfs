# Shared helpers for Raft docker-compose scripts (source from bash, do not execute directly).
: "${ROOT:?ROOT must be set}"

# Host-published ports for master-0..2 (see docker-compose.raft.yml).
RAFT_GRPC_PORTS=(9090 9093 9095)
RAFT_METRICS_PORTS=(9091 9094 9096)

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

# Echo host:port of the current Raft leader (127.0.0.1 + published port).
raft_leader_grpc_addr() {
  local idx
  idx="$(raft_find_leader_index)" || return 1
  echo "127.0.0.1:${RAFT_GRPC_PORTS[$idx]}"
}

# Map host-published master gRPC (127.0.0.1:909x) to compose DNS (master-N:9090).
host_master_to_compose_master() {
  local host="$1"
  local i
  for i in "${!RAFT_GRPC_PORTS[@]}"; do
    if [[ "127.0.0.1:${RAFT_GRPC_PORTS[$i]}" == "$host" ]]; then
      echo "master-${i}:9090"
      return 0
    fi
  done
  return 1
}

compose_master_grpc_addr() {
  local idx
  idx="$(raft_find_leader_index)" || return 1
  echo "master-${idx}:9090"
}

# Data-plane CLI on the compose network (chunk advertise uses service DNS names).
# Requires GODFS_COMPOSE_EXTRA_FILE when chunks use in-network advertise (release-checklist overlay).
godfs_client_compose() {
  local master="${1:?}"
  shift
  local base="${GODFS_RAFT_COMPOSE_FILE:-deployments/docker/docker-compose.raft.yml}"
  local extra="${GODFS_COMPOSE_EXTRA_FILE:-}"
  local -a run=(docker compose -f "$base")
  if [[ -n "$extra" ]]; then
    run+=(-f "$extra")
  fi
  local tmp="${TMPDIR:-/tmp}"
  local vol=()
  if [[ -d "$tmp" ]]; then
    vol=(-v "${tmp}:${tmp}")
  fi
  "${run[@]}" run --rm --no-deps -T --user "$(id -u):$(id -g)" "${vol[@]}" \
    client --master "$master" "$@"
}

# Wait until ListChunkNodes reports at least one alive node on the given master gRPC address.
wait_chunk_alive() {
  local master_grpc="$1"
  local timeout_sec="${2:-120}"
  local deadline=$((SECONDS + timeout_sec))
  while ((SECONDS < deadline)); do
    if godfs_client --master "$master_grpc" nodes 2>/dev/null | grep -qE $'\t(alive|dead)$'; then
      return 0
    fi
    sleep 2
  done
  echo "no alive chunk node on leader ${master_grpc} within ${timeout_sec}s" >&2
  return 1
}

# Wait until at least min_alive chunk nodes report alive on the leader.
wait_chunks_alive_min() {
  local master_grpc="$1"
  local min_alive="$2"
  local timeout_sec="${3:-180}"
  local deadline=$((SECONDS + timeout_sec))
  local n
  while ((SECONDS < deadline)); do
    n=$(godfs_client --master "$master_grpc" nodes 2>/dev/null | grep -c $'\talive$' || true)
    if [[ "${n:-0}" -ge "$min_alive" ]]; then
      return 0
    fi
    sleep 2
  done
  echo "fewer than ${min_alive} alive chunk nodes on ${master_grpc} within ${timeout_sec}s" >&2
  return 1
}

# Echo 0..N-1 index of the master that answers masters list as leader; exit 1 if none.
# Caller may set GRPC_PORTS / METRICS_PORTS; otherwise RAFT_* defaults apply.
raft_find_leader_index() {
  local i addr out leader
  local -a grpc_ports metrics_ports
  if [[ -v GRPC_PORTS ]]; then
    grpc_ports=("${GRPC_PORTS[@]}")
  else
    grpc_ports=("${RAFT_GRPC_PORTS[@]}")
  fi
  if [[ -v METRICS_PORTS ]]; then
    metrics_ports=("${METRICS_PORTS[@]}")
  else
    metrics_ports=("${RAFT_METRICS_PORTS[@]}")
  fi
  for i in "${!grpc_ports[@]}"; do
    addr="127.0.0.1:${grpc_ports[$i]}"
    if out=$(godfs_client --master "$addr" masters list 2>/dev/null); then
      leader=$(grep -m1 '^leader_node_id=' <<<"$out" | cut -d= -f2- | tr -d '\r\n')
      if [[ -n "$leader" ]]; then
        echo "$i"
        return 0
      fi
    fi
  done
  for i in "${!metrics_ports[@]}"; do
    if metrics_is_leader "${metrics_ports[$i]}"; then
      echo "$i"
      return 0
    fi
  done
  return 1
}
