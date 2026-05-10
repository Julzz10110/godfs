# Full data DR: chunk bytes backup/restore

This document describes **full data disaster recovery** for goDFS: backing up and restoring **chunk bytes** (the `*.chk` files stored on ChunkServers).

It complements metadata DR (`BackupManifest` snapshots). Metadata-only DR is not sufficient if chunk disks are lost.

## Scope and goals

- **Goal**: be able to restore user data even after losing ChunkServer nodes/disks, not only Master metadata.
- **Out of scope (initial baseline)**:
  - cross-region replication automation
  - continuous “point-in-time” consistency across metadata + chunk bytes
  - S3/MinIO backend (planned next after filesystem baseline)

## Consistency model (what can be guaranteed)

Chunk bytes live on ChunkServers; metadata lives on Masters.

For a **crash-consistent** backup, you need:

- A metadata snapshot (`BackupManifest`) that defines the desired chunk set and expected checksums.
- A chunk bytes backup that contains the referenced `chunk_id` objects.

### Recommended operational procedure (baseline)

1) **Quiesce writes** (best effort):
   - stop/deny client writes (operator action)
   - wait for in-flight writes to finish

2) Create metadata snapshot:

```bash
godfs-client --master <master:9090> snapshot create "dr-<ts>"
godfs-client --master <master:9090> snapshot get <snapshot_id> manifest.json
```

3) Backup chunk bytes from all ChunkServers:
   - run a per-node backup job that copies `dataDir/*.chk` to a DR location
   - record per-chunk checksums in an index file

4) Store both artifacts (`manifest.json` + chunk backups) in external storage with retention.

### What happens if writes are not quiesced?

You may capture a `manifest.json` that references a checksum different from the chunk bytes you backed up (or vice versa). The restore tooling must **verify checksums** and report mismatches.

## Backup format (filesystem baseline)

We use a directory layout:

```text
<backupRoot>/
  index.jsonl
  chunks/
    <chunk_id>.chk
    ...
```

- `chunks/<chunk_id>.chk`: raw bytes copied from the ChunkServer dataDir.
- `index.jsonl`: one JSON object per chunk:
  - `chunk_id`
  - `size_bytes`
  - `checksum_sha256_hex`
  - `mod_unix` (best-effort)

## Restore workflow

1) Provision new ChunkServers with empty data directories.
2) Restore chunk bytes into the new ChunkServer `dataDir`:
   - copy `chunks/<chunk_id>.chk` into `dataDir/<chunk_id>.chk`
   - verify SHA-256 matches `index.jsonl`
3) Restore metadata from `manifest.json` into Masters (see `docs/DR_BACKUP_RESTORE.md`).
4) Run validation reads / allow background repair to converge.

## Tooling baseline

We implement a local CLI that operates on a ChunkServer filesystem:

- `chunkctl backup --data-dir <dir> --out <backupRoot>`
- `chunkctl restore --data-dir <dir> --in <backupRoot> [--overwrite]`

Both commands:

- are **offline-friendly**
- compute/verify SHA-256
- are safe by default (refuse overwrites unless requested)

## Validation checklist

- After restore, for a sample of files:
  - `godfs-client stat /path`
  - read via SDK/REST and compare expected content
- Optionally, verify every chunk referenced in `manifest.json` exists on at least one restored ChunkServer.

