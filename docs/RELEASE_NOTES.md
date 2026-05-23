# Release notes

Template for tagged releases. Copy the **Unreleased** section into a version heading when cutting a tag.

## Unreleased

### Highlights

- Distributed FS with Raft metadata, 3× chunk replication (configurable), REST `/v1`, Linux FUSE, operator CLI, snapshots, presigned GET/PUT, multipart uploads.

### Upgrade / operations

- First production cluster: [`docs/K8S_PRODUCTION.md`](K8S_PRODUCTION.md), [`deployments/k8s/OPERATIONS.md`](../deployments/k8s/OPERATIONS.md).
- Release acceptance: [`docs/RELEASE_CHECKLIST.md`](RELEASE_CHECKLIST.md).

### Known limitations

- **FUSE:** Linux only (`cmd/fuse`); no macOS / WinFsp / WebDAV in this release.
- **Master without Raft env:** single in-memory metadata node (dev only); production requires Raft (`GODFS_MASTER_RAFT_*`).
- **Replication:** client-visible success after primary + `CommitChunk`; secondaries catch up via `SyncChunk` (not “all replicas before ACK”).
- **REST multipart:** staging on gateway disk (`GODFS_REST_MULTIPART_DIR`); size/part limits via env (see [`docs/ENV_REFERENCE.md`](ENV_REFERENCE.md)).
- **Soft-delete grace:** optional; strict GC and health scan behavior documented in [`docs/RUNBOOK.md`](RUNBOOK.md).

### Breaking changes

- None for REST `/v1` in this template (future breaking changes use `/v2`).

---

## v1.0.0 (example)

_Fill when tagging._

- [ ] R1–R10 completed per [`RELEASE_CHECKLIST.md`](RELEASE_CHECKLIST.md)
- [ ] CI commit: _sha_
- [ ] Image tags: _list_
