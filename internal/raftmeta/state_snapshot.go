package raftmeta

import (
	"sort"
	"time"

	"godfs/internal/domain"
)

func (s *State) ensureSnapshots() {
	if s.Snapshots == nil {
		s.Snapshots = map[string]*domain.BackupSnapshot{}
	}
}

// CreateBackupSnapshot records a point-in-time manifest from current namespace + chunks.
func (s *State) CreateBackupSnapshot(id, label string, at time.Time) error {
	s.ensureSnapshots()
	if _, ok := s.Snapshots[id]; ok {
		return domain.ErrAlreadyExists
	}
	man := s.buildBackupSnapshot(id, label, at)
	s.Snapshots[id] = man
	return nil
}

func (s *State) buildBackupSnapshot(id, label string, at time.Time) *domain.BackupSnapshot {
	paths := make([]string, 0, len(s.Files))
	for p := range s.Files {
		paths = append(paths, p)
	}
	sort.Strings(paths)

	out := &domain.BackupSnapshot{
		ID:                id,
		Label:             label,
		CreatedAt:         at.UTC(),
		ChunkSize:         s.ChunkSize,
		ReplicationFactor: s.ReplicationFactor,
	}
	for _, p := range paths {
		fr := s.Files[p]
		fe := domain.BackupFileEntry{
			Path:       p,
			Size:       fr.Size,
			Mode:       fr.Mode,
			CreatedAt:  fr.Created.UTC(),
			ModifiedAt: fr.Modified.UTC(),
		}
		for i, cid := range fr.Chunks {
			cr := s.Chunks[cid]
			if cr == nil {
				continue
			}
			reps := append([]domain.ChunkReplica(nil), cr.Replicas...)
			sum := append([]byte(nil), cr.Checksum...)
			fe.Chunks = append(fe.Chunks, domain.BackupChunkRef{
				ChunkID:    cid,
				ChunkIndex: int64(i),
				Version:    cr.Version,
				Checksum:   sum,
				Replicas:   reps,
			})
		}
		out.Files = append(out.Files, fe)
	}
	return out
}

// DeleteBackupSnapshot removes a stored manifest.
func (s *State) DeleteBackupSnapshot(id string) error {
	s.ensureSnapshots()
	if _, ok := s.Snapshots[id]; !ok {
		return domain.ErrNotFound
	}
	delete(s.Snapshots, id)
	return nil
}

// ListBackupSnapshots returns listing metadata.
func (s *State) ListBackupSnapshots() []domain.SnapshotInfo {
	s.ensureSnapshots()
	ids := make([]string, 0, len(s.Snapshots))
	for id := range s.Snapshots {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]domain.SnapshotInfo, 0, len(ids))
	for _, id := range ids {
		sn := s.Snapshots[id]
		if sn == nil {
			continue
		}
		out = append(out, domain.SnapshotInfo{
			ID:            id,
			Label:         sn.Label,
			CreatedAtUnix: sn.CreatedAt.Unix(),
			FileCount:     int32(len(sn.Files)),
		})
	}
	return out
}

// GetBackupSnapshot returns a deep copy of the manifest.
func (s *State) GetBackupSnapshot(id string) (*domain.BackupSnapshot, error) {
	s.ensureSnapshots()
	sn, ok := s.Snapshots[id]
	if !ok || sn == nil {
		return nil, domain.ErrNotFound
	}
	cp := *sn
	cp.Files = append([]domain.BackupFileEntry(nil), sn.Files...)
	for i := range cp.Files {
		f := &cp.Files[i]
		f.Chunks = append([]domain.BackupChunkRef(nil), f.Chunks...)
		for j := range f.Chunks {
			c := &f.Chunks[j]
			c.Checksum = append([]byte(nil), c.Checksum...)
			c.Replicas = append([]domain.ChunkReplica(nil), c.Replicas...)
		}
	}
	return &cp, nil
}
