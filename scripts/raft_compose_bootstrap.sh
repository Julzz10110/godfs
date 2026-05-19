#!/usr/bin/env bash
# Bootstrap 1-node Raft on master-0, then join master-1 and master-2 (matches e2e TestE2E_RaftMaster_*).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

MASTER0_GRPC="${GODFS_RAFT_BOOTSTRAP_MASTER:-127.0.0.1:9090}"
TIMEOUT_SEC="${GODFS_RAFT_BOOTSTRAP_TIMEOUT:-120}"

echo "Waiting for Raft leader on master-0 (${MASTER0_GRPC}) ..."
GODFS_RAFT_BOOTSTRAP_MASTER="$MASTER0_GRPC" \
  GODFS_RAFT_LEADER_WAIT_TIMEOUT="$TIMEOUT_SEC" \
  bash scripts/wait_raft_leader.sh

join_master() {
  local id="$1" raft="$2" grpc="$3"
  echo "Adding master ${id} (${raft} / ${grpc})"
  go run ./cmd/client --master "$MASTER0_GRPC" masters add "$id" "$raft" "$grpc"
}

join_master master-1 master-1:9200 master-1:9090
join_master master-2 master-2:9200 master-2:9090

sleep 3
GODFS_RAFT_BOOTSTRAP_MASTER="$MASTER0_GRPC" \
  GODFS_RAFT_LEADER_WAIT_TIMEOUT="${GODFS_RAFT_POST_JOIN_LEADER_TIMEOUT:-60}" \
  bash scripts/wait_raft_leader.sh
echo "raft compose bootstrap OK"
