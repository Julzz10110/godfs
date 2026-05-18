package raftmeta

import (
	"time"

	"godfs/internal/dataplane"
	"godfs/internal/domain"
)

func (s *State) fileVisible(fr *fileRec, at time.Time) bool {
	if fr == nil {
		return false
	}
	return dataplane.FileVisibleForAPI(fr.DeletedAtUnix, at, s.SoftDeleteGrace)
}

// RestoreFile clears a soft-delete tombstone while still inside the grace window.
func (s *State) RestoreFile(p string, at time.Time) error {
	fp, err := normalizePath(p)
	if err != nil {
		return err
	}
	if s.SoftDeleteGrace <= 0 {
		return domain.ErrNotFound
	}
	fr, ok := s.Files[fp]
	if !ok || fr.DeletedAtUnix == 0 {
		return domain.ErrNotFound
	}
	if !dataplane.FileInTrash(fr.DeletedAtUnix, at, s.SoftDeleteGrace) {
		return domain.ErrNotFound
	}
	fr.DeletedAtUnix = 0
	return nil
}

// PurgeExpiredSoftDeletes hard-deletes files past the trash grace window.
func (s *State) PurgeExpiredSoftDeletes(at time.Time) {
	if s.SoftDeleteGrace <= 0 {
		return
	}
	var purge []string
	for fp, fr := range s.Files {
		if dataplane.FileReadyToPurge(fr.DeletedAtUnix, at, s.SoftDeleteGrace) {
			purge = append(purge, fp)
		}
	}
	for _, fp := range purge {
		_, _ = s.hardDeleteFile(fp)
	}
}
