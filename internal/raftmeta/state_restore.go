package raftmeta

import (
	"fmt"
	"path"
	"time"

	"godfs/internal/domain"
)

// RestoreSnapshot rebuilds namespace metadata from a backup manifest.
// If force is false, restore is only allowed when the namespace is empty.
func (s *State) RestoreSnapshot(manifest *domain.BackupSnapshot, force bool) error {
	if manifest == nil {
		return fmt.Errorf("manifest required")
	}
	if manifest.ChunkSize <= 0 {
		return fmt.Errorf("invalid manifest chunk_size=%d", manifest.ChunkSize)
	}
	if manifest.ReplicationFactor <= 0 {
		return fmt.Errorf("invalid manifest replication_factor=%d", manifest.ReplicationFactor)
	}

	empty := len(s.Files) == 0 && len(s.Dirs) <= 1 && len(s.Chunks) == 0
	if !empty && !force {
		return fmt.Errorf("refusing to restore into non-empty namespace (use force=true)")
	}

	s.ChunkSize = manifest.ChunkSize
	s.ReplicationFactor = manifest.ReplicationFactor

	// Reset namespace/chunk maps. Keep Nodes/NodeStatus: operators may already have nodes registered.
	s.Dirs = map[string]struct{}{"/": {}}
	s.Files = map[string]*fileRec{}
	s.Chunks = map[domain.ChunkID]*chunkRec{}
	s.PendingDeletes = map[domain.ChunkID]map[string]*pendingDelete{}
	s.RebalanceTasks = map[domain.ChunkID]*rebalanceTask{}
	if s.NodeUsedBytes == nil {
		s.NodeUsedBytes = map[domain.NodeID]int64{}
	}
	for id := range s.NodeUsedBytes {
		s.NodeUsedBytes[id] = 0
	}

	for _, fe := range manifest.Files {
		fp, err := normalizePath(fe.Path)
		if err != nil {
			return fmt.Errorf("file path %q: %w", fe.Path, err)
		}
		parent := path.Dir(fp)
		for parent != "." && parent != "/" {
			s.Dirs[parent] = struct{}{}
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
			ID:       domain.FileID(""),
			Chunks:   nil,
			Size:     fe.Size,
			Created:  created,
			Modified: modified,
			Mode:     fe.Mode,
		}

		var maxIdx int64 = -1
		for _, cref := range fe.Chunks {
			if cref.ChunkIndex > maxIdx {
				maxIdx = cref.ChunkIndex
			}
		}
		if maxIdx >= 0 {
			fr.Chunks = make([]domain.ChunkID, maxIdx+1)
		}

		for _, cref := range fe.Chunks {
			if cref.ChunkIndex < 0 {
				return fmt.Errorf("file %q: invalid chunk_index=%d", fp, cref.ChunkIndex)
			}
			if int64(len(fr.Chunks)) <= cref.ChunkIndex {
				tmp := make([]domain.ChunkID, cref.ChunkIndex+1)
				copy(tmp, fr.Chunks)
				fr.Chunks = tmp
			}
			cid := cref.ChunkID
			if cid == "" {
				return fmt.Errorf("file %q: empty chunk_id at index=%d", fp, cref.ChunkIndex)
			}
			if fr.Chunks[cref.ChunkIndex] != "" && fr.Chunks[cref.ChunkIndex] != cid {
				return fmt.Errorf("file %q: duplicate chunk_index=%d", fp, cref.ChunkIndex)
			}
			fr.Chunks[cref.ChunkIndex] = cid

			if _, ok := s.Chunks[cid]; !ok {
				reps := append([]domain.ChunkReplica(nil), cref.Replicas...)
				sum := append([]byte(nil), cref.Checksum...)
				s.Chunks[cid] = &chunkRec{
					ID:       cid,
					Replicas: reps,
					Version:  cref.Version,
					Checksum: sum,
				}
				for _, r := range reps {
					s.NodeUsedBytes[r.NodeID] += s.ChunkSize
				}
			}
		}

		if _, exists := s.Files[fp]; exists {
			return fmt.Errorf("duplicate file path %q in manifest", fp)
		}
		s.Files[fp] = fr
	}

	if s.Snapshots == nil {
		s.Snapshots = map[string]*domain.BackupSnapshot{}
	}
	if manifest.ID != "" {
		cp := *manifest
		cp.Files = append([]domain.BackupFileEntry(nil), manifest.Files...)
		s.Snapshots[manifest.ID] = &cp
	}
	return nil
}
