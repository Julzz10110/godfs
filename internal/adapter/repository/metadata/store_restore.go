package metadata

import (
	"context"
	"fmt"
	"path"
	"time"

	"godfs/internal/domain"
)

// RestoreSnapshot rebuilds namespace metadata from a backup manifest (single-master mode).
// Chunk bytes are not copied; replicas are used as hints for reads/repair.
func (s *Store) RestoreSnapshot(_ context.Context, manifest *domain.BackupSnapshot, force bool) error {
	if manifest == nil {
		return fmt.Errorf("manifest required")
	}
	if manifest.ChunkSize <= 0 {
		return fmt.Errorf("invalid manifest chunk_size=%d", manifest.ChunkSize)
	}
	if manifest.ReplicationFactor <= 0 {
		return fmt.Errorf("invalid manifest replication_factor=%d", manifest.ReplicationFactor)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	empty := len(s.files) == 0 && len(s.dirs) <= 1 && len(s.chunks) == 0
	if !empty && !force {
		return fmt.Errorf("refusing to restore into non-empty namespace (use force=true)")
	}

	// Ensure config is compatible; allow setting on empty/force restore.
	s.chunkSize = manifest.ChunkSize
	s.replicationFactor = manifest.ReplicationFactor

	// Reset namespace/chunk maps.
	s.dirs = map[string]struct{}{"/": {}}
	s.files = map[string]*fileRec{}
	s.chunks = map[domain.ChunkID]*chunkRec{}
	s.pendingDeletes = map[domain.ChunkID]map[string]*pendingChunkDelete{}
	s.rebalanceTasks = map[domain.ChunkID]*rebalanceWork{}
	// Keep nodes/heartbeats: they are ephemeral ops telemetry; restoring them from backup is not required.
	// Recompute used-bytes estimate from restored chunk refs.
	if s.nodeUsedBytes == nil {
		s.nodeUsedBytes = map[domain.NodeID]int64{}
	}
	for id := range s.nodeUsedBytes {
		s.nodeUsedBytes[id] = 0
	}

	for _, fe := range manifest.Files {
		fp, err := normalizePath(fe.Path)
		if err != nil {
			return fmt.Errorf("file path %q: %w", fe.Path, err)
		}
		// Ensure all parent dirs exist.
		parent := path.Dir(fp)
		for parent != "." && parent != "/" {
			s.dirs[parent] = struct{}{}
			parent = path.Dir(parent)
		}

		created := fe.CreatedAt.UTC()
		modified := fe.ModifiedAt.UTC()
		if created.IsZero() {
			created = time.Unix(0, 0).UTC()
		}
		if modified.IsZero() {
			modified = created
		}

		fr := &fileRec{
			id:       domain.FileID(""), // restored manifests do not include file_id; it is not exposed via API.
			chunks:   nil,
			size:     fe.Size,
			created:  created,
			modified: modified,
			mode:     fe.Mode,
		}

		// Build chunk sequence.
		var maxIdx int64 = -1
		for _, cref := range fe.Chunks {
			if cref.ChunkIndex > maxIdx {
				maxIdx = cref.ChunkIndex
			}
		}
		if maxIdx >= 0 {
			fr.chunks = make([]domain.ChunkID, maxIdx+1)
		}
		for _, cref := range fe.Chunks {
			if cref.ChunkIndex < 0 {
				return fmt.Errorf("file %q: invalid chunk_index=%d", fp, cref.ChunkIndex)
			}
			if int64(len(fr.chunks)) <= cref.ChunkIndex {
				// Should not happen, but keep safe.
				tmp := make([]domain.ChunkID, cref.ChunkIndex+1)
				copy(tmp, fr.chunks)
				fr.chunks = tmp
			}
			cid := cref.ChunkID
			if cid == "" {
				return fmt.Errorf("file %q: empty chunk_id at index=%d", fp, cref.ChunkIndex)
			}
			if fr.chunks[cref.ChunkIndex] != "" && fr.chunks[cref.ChunkIndex] != cid {
				return fmt.Errorf("file %q: duplicate chunk_index=%d", fp, cref.ChunkIndex)
			}
			fr.chunks[cref.ChunkIndex] = cid

			// Materialize chunk record once per chunk id.
			if _, ok := s.chunks[cid]; !ok {
				reps := append([]domain.ChunkReplica(nil), cref.Replicas...)
				sum := append([]byte(nil), cref.Checksum...)
				s.chunks[cid] = &chunkRec{
					id:       cid,
					replicas: reps,
					version:  cref.Version,
					checksum: sum,
				}
				// Placement estimate for replica nodes (even if node isn't registered yet).
				for _, r := range reps {
					s.nodeUsedBytes[r.NodeID] += s.chunkSize
				}
			}
		}

		if _, exists := s.files[fp]; exists {
			return fmt.Errorf("duplicate file path %q in manifest", fp)
		}
		s.files[fp] = fr
	}

	// Record the manifest as a snapshot entry (best-effort audit trail).
	if s.snapshots == nil {
		s.snapshots = map[string]*domain.BackupSnapshot{}
	}
	if manifest.ID != "" {
		cp := *manifest
		cp.Files = append([]domain.BackupFileEntry(nil), manifest.Files...)
		s.snapshots[manifest.ID] = &cp
	}
	return nil
}
