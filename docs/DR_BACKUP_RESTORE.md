# DR backup/restore (metadata)

This document describes a minimal, practical DR process for goDFS based on `BackupManifest` (snapshot).

## What is backed up

- A snapshot (`BackupManifest`) contains **metadata**: a list of paths, file attributes, and chunk references (id/version/checksum) plus replica hints (node_id/grpc_address).
- **Chunk bytes are not copied**. Full data DR requires chunk disks to be available (same nodes/disks, or a separate chunk backup at the volume/object-store layer).

## Invariants and expectations

- **Restore** rebuilds the namespace (dirs/files/chunks) from the manifest.
- By default, restore is allowed **only into an empty namespace**.
- To restore on top of existing metadata, use `--force` (it clears current metadata before restore).
- In Raft mode, restore runs **only on the leader** (like other admin operations); calling a follower returns `not leader`.

## Backup: create and export a manifest

1) Create a snapshot:

```bash
godfs-client --master <master-grpc:9090> snapshot create "daily-2026-05-10"
```

2) Export the manifest to JSON:

```bash
godfs-client --master <master-grpc:9090> snapshot get <snapshot_id> manifest.json
```

3) Store `manifest.json` in external storage:

- **minimum**: PVC/NFS in a different failure domain;
- **better**: object storage (S3/MinIO) with versioning/immutability;
- keep multiple generations: daily/weekly/monthly.

## Restore: restore metadata from a manifest

1) Deploy a new master cluster with **empty** volumes (or make sure the namespace is empty).

2) Apply restore:

```bash
godfs-client --master <master-grpc:9090> snapshot restore manifest.json
```

If you need to overwrite existing metadata:

```bash
godfs-client --master <master-grpc:9090> snapshot restore manifest.json --force
```

3) After restore:

- ensure ChunkServer nodes are reachable at the addresses from the manifest;
- run reads/checks; if needed, let the background rebalancer/GC converge the cluster to the desired state.

## Recommended minimal operator test plan

- In a source cluster: create a file, write data, create a snapshot, export `manifest.json`.
- In a new/empty master cluster: restore from `manifest.json`.
- Verify: `stat/list/getchunk` on restored paths, then read via client/REST gateway.

