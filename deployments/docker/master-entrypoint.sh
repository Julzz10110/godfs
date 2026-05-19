#!/bin/sh
set -eu

# Ensure the container hostname resolves before binding Raft to host:port (GODFS_MASTER_RAFT_LISTEN).
node="${GODFS_MASTER_NODE_ID:-}"
raft_listen="${GODFS_MASTER_RAFT_LISTEN:-}"
if [ -n "$node" ] && [ -n "$raft_listen" ]; then
  case "$raft_listen" in
    "${node}:"*)
      i=0
      while [ "$i" -lt 60 ]; do
        if getent hosts "$node" >/dev/null 2>&1; then
          break
        fi
        i=$((i + 1))
        sleep 0.1
      done
      ;;
  esac
fi

exec /usr/local/bin/godfs-master
