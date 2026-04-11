package metadata

import (
	"context"
	"sort"
	"time"

	"github.com/google/uuid"

	"godfs/internal/domain"
)

// CreateSnapshot records a backup manifest (single-master mode).
func (s *Store) CreateSnapshot(_ context.Context, label string) (string, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snapshots == nil {
		s.snapshots = map[string]*domain.BackupSnapshot{}
	}
	id := uuid.New().String()
	at := time.Now().UTC()
	man := s.buildBackupSnapshot(id, label, at)
	s.snapshots[id] = man
	return id, at.Unix(), nil
}

func (s *Store) buildBackupSnapshot(id, label string, at time.Time) *domain.BackupSnapshot {
	paths := make([]string, 0, len(s.files))
	for p := range s.files {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	out := &domain.BackupSnapshot{
		ID:                id,
		Label:             label,
		CreatedAt:         at.UTC(),
		ChunkSize:         s.chunkSize,
		ReplicationFactor: s.replicationFactor,
	}
	for _, p := range paths {
		fr := s.files[p]
		fe := domain.BackupFileEntry{
			Path:       p,
			Size:       fr.size,
			Mode:       fr.mode,
			CreatedAt:  fr.created.UTC(),
			ModifiedAt: fr.modified.UTC(),
		}
		for i, cid := range fr.chunks {
			cr := s.chunks[cid]
			if cr == nil {
				continue
			}
			reps := append([]domain.ChunkReplica(nil), cr.replicas...)
			sum := append([]byte(nil), cr.checksum...)
			fe.Chunks = append(fe.Chunks, domain.BackupChunkRef{
				ChunkID:    cid,
				ChunkIndex: int64(i),
				Version:    cr.version,
				Checksum:   sum,
				Replicas:   reps,
			})
		}
		out.Files = append(out.Files, fe)
	}
	return out
}

// ListSnapshots implements [usecase.MasterStore].
func (s *Store) ListSnapshots(_ context.Context) ([]domain.SnapshotInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snapshots == nil {
		return nil, nil
	}
	ids := make([]string, 0, len(s.snapshots))
	for id := range s.snapshots {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]domain.SnapshotInfo, 0, len(ids))
	for _, id := range ids {
		sn := s.snapshots[id]
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
	return out, nil
}

// GetSnapshot implements [usecase.MasterStore].
func (s *Store) GetSnapshot(_ context.Context, id string) (*domain.BackupSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snapshots == nil {
		return nil, domain.ErrNotFound
	}
	sn, ok := s.snapshots[id]
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

// DeleteSnapshot implements [usecase.MasterStore].
func (s *Store) DeleteSnapshot(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snapshots == nil {
		return domain.ErrNotFound
	}
	if _, ok := s.snapshots[id]; !ok {
		return domain.ErrNotFound
	}
	delete(s.snapshots, id)
	return nil
}
