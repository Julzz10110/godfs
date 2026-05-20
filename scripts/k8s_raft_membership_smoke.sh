#!/usr/bin/env bash
# Raft membership smoke: ListMasters (+ optional add/remove in lab mode).
#
# Required:
#   GODFS_MASTER_ADDR   host:port of master gRPC (e.g. 127.0.0.1:9090 after kubectl port-forward)
#
# Optional:
#   GODFS_CLIENT_BIN    path to godfs-client (default: bin/godfs-client)
#   GODFS_CLIENT_API_KEY / GODFS_CLUSTER_KEY — passed via godfs-client env if set
#   GODFS_MEMBERSHIP_LAB=1 — run add then remove for a throwaway node (destructive; lab only)
#   GODFS_MEMBERSHIP_LAB_NODE_ID  default: godfs-master-lab
#   GODFS_MEMBERSHIP_LAB_RAFT_ADDR / GODFS_MEMBERSHIP_LAB_GRPC_ADDR — required when LAB=1
#
# Example (port-forward):
#   kubectl -n godfs port-forward svc/godfs-master 9090:9090 &
#   export GODFS_MASTER_ADDR=127.0.0.1:9090
#   export GODFS_CLIENT_API_KEY="$(kubectl -n godfs get secret godfs-auth -o jsonpath='{.data.GODFS_API_KEYS}' | base64 -d | cut -d= -f2)"
#   bash scripts/k8s_raft_membership_smoke.sh
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

MASTER="${GODFS_MASTER_ADDR:?set GODFS_MASTER_ADDR (host:port)}"
CLIENT="${GODFS_CLIENT_BIN:-${ROOT}/bin/godfs-client}"

if [[ ! -x "$CLIENT" ]] && [[ ! -f "$CLIENT" ]]; then
  echo "godfs-client not found at $CLIENT (go build -o bin/godfs-client ./cmd/client)" >&2
  exit 1
fi

godfs_client() {
  "$CLIENT" --master "$MASTER" "$@"
}

out="$(godfs_client masters list)"
echo "$out"

leader="$(echo "$out" | awk -F= '/^leader_node_id=/ {print $2}')"
if [[ -z "$leader" ]]; then
  echo "no leader_node_id in ListMasters output" >&2
  exit 1
fi

voters="$(echo "$out" | awk '$NF=="voter" {c++} END {print c+0}')"
if [[ "$voters" -lt 3 ]]; then
  echo "expected at least 3 voters, got $voters" >&2
  exit 1
fi

if [[ "${GODFS_MEMBERSHIP_LAB:-}" != "1" ]]; then
  echo "membership smoke OK (list + quorum check)"
  exit 0
fi

node_id="${GODFS_MEMBERSHIP_LAB_NODE_ID:-godfs-master-lab}"
raft_addr="${GODFS_MEMBERSHIP_LAB_RAFT_ADDR:?set GODFS_MEMBERSHIP_LAB_RAFT_ADDR for lab add/remove}"
grpc_addr="${GODFS_MEMBERSHIP_LAB_GRPC_ADDR:?set GODFS_MEMBERSHIP_LAB_GRPC_ADDR for lab add/remove}"

echo "LAB: AddMaster $node_id"
godfs_client masters add "$node_id" "$raft_addr" "$grpc_addr"
godfs_client masters list

echo "LAB: RemoveMaster $node_id"
godfs_client masters remove "$node_id"
godfs_client masters list

echo "membership lab smoke OK"
