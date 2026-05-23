package usecase

import (
	"context"
	"testing"
)

func TestTruncateFile_normalizesPath(t *testing.T) {
	m := &mockDeleteStore{}
	_, err := TruncateFile(context.Background(), m, "/a/../b", 10)
	if err != nil {
		t.Fatal(err)
	}
	if m.truncatePath != "/b" || m.truncateSize != 10 {
		t.Fatalf("path=%q size=%d", m.truncatePath, m.truncateSize)
	}
}

func TestTruncateFile_invalidSize(t *testing.T) {
	m := &mockDeleteStore{}
	_, err := TruncateFile(context.Background(), m, "/x", -1)
	if err == nil {
		t.Fatal("expected error")
	}
}
