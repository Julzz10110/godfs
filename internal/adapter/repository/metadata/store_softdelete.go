package metadata

import (
	"context"
	"time"

	"godfs/internal/dataplane"
	"godfs/internal/domain"
)

func (s *Store) fileVisibleLocked(fr *fileRec, at time.Time) bool {
	if fr == nil {
		return false
	}
	return dataplane.FileVisibleForAPI(fr.deletedAtUnix, at, s.softDeleteGrace)
}

// RestoreFile clears a soft-delete tombstone while still inside GODFS_SOFT_DELETE_GRACE.
func (s *Store) RestoreFile(_ context.Context, p string) error {
	fp, err := normalizePath(p)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.softDeleteGrace <= 0 {
		return domain.ErrNotFound
	}
	fr, ok := s.files[fp]
	if !ok || fr.deletedAtUnix == 0 {
		return domain.ErrNotFound
	}
	at := time.Now().UTC()
	if !dataplane.FileInTrash(fr.deletedAtUnix, at, s.softDeleteGrace) {
		return domain.ErrNotFound
	}
	fr.deletedAtUnix = 0
	return nil
}

// PurgeExpiredSoftDeletes hard-deletes files whose trash grace has elapsed.
func (s *Store) PurgeExpiredSoftDeletes(at time.Time) {
	if s.softDeleteGrace <= 0 {
		return
	}
	var purge []string
	s.mu.Lock()
	for fp, fr := range s.files {
		if dataplane.FileReadyToPurge(fr.deletedAtUnix, at, s.softDeleteGrace) {
			purge = append(purge, fp)
		}
	}
	s.mu.Unlock()
	for _, fp := range purge {
		_, _ = s.deleteFile(fp)
	}
}
