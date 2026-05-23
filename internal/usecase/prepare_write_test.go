package usecase

import (
	"context"
	"errors"
	"testing"

	"godfs/internal/domain"
)

func TestPrepareWrite_invalidLength(t *testing.T) {
	m := &mockDeleteStore{}
	_, _, _, _, _, _, _, _, _, err := PrepareWrite(context.Background(), m, "/a", 0, 0)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPrepareWrite_invalidPath(t *testing.T) {
	m := &mockDeleteStore{}
	_, _, _, _, _, _, _, _, _, err := PrepareWrite(context.Background(), m, "x", 0, 1)
	if !errors.Is(err, domain.ErrInvalidPath) {
		t.Fatalf("got %v", err)
	}
}

// mockDeleteStore from delete_file_test.go is reused.
