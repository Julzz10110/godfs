package dataplane

import "time"

// FileInTrash reports whether a file is soft-deleted and still within the grace window.
func FileInTrash(deletedAtUnix int64, at time.Time, grace time.Duration) bool {
	if deletedAtUnix <= 0 || grace <= 0 {
		return false
	}
	return !at.After(time.Unix(deletedAtUnix, 0).UTC().Add(grace))
}

// FileVisibleForAPI is false when the file is in the trash (hidden from list/stat/read).
func FileVisibleForAPI(deletedAtUnix int64, at time.Time, grace time.Duration) bool {
	return !FileInTrash(deletedAtUnix, at, grace)
}

// FileReadyToPurge is true when soft-delete grace has elapsed.
func FileReadyToPurge(deletedAtUnix int64, at time.Time, grace time.Duration) bool {
	if deletedAtUnix <= 0 || grace <= 0 {
		return false
	}
	return at.After(time.Unix(deletedAtUnix, 0).UTC().Add(grace))
}
