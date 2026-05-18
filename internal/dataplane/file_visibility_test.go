package dataplane

import (
	"testing"
	"time"
)

func TestFileInTrashAndPurge(t *testing.T) {
	grace := time.Hour
	deleted := time.Now().Add(-30 * time.Minute).Unix()
	if !FileInTrash(deleted, time.Now(), grace) {
		t.Fatal("should be in trash")
	}
	if FileVisibleForAPI(deleted, time.Now(), grace) {
		t.Fatal("should be hidden from API")
	}
	if FileReadyToPurge(deleted, time.Now(), grace) {
		t.Fatal("not ready to purge yet")
	}
	if !FileReadyToPurge(deleted, time.Now().Add(2*time.Hour), grace) {
		t.Fatal("should purge after grace")
	}
}
