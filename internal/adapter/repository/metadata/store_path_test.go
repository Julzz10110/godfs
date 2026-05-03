package metadata

import (
	"context"
	"errors"
	"testing"

	"godfs/internal/domain"
)

func TestStorePathValidationAndErrors(t *testing.T) {
	ctx := context.Background()
	s := NewStore(1024, 1)

	if _, err := s.CreateFile(ctx, "relative"); !errors.Is(err, domain.ErrInvalidPath) {
		t.Fatalf("CreateFile relative: got %v want %v", err, domain.ErrInvalidPath)
	}
	if err := s.Mkdir(ctx, ""); !errors.Is(err, domain.ErrInvalidPath) {
		t.Fatalf("Mkdir empty: got %v want %v", err, domain.ErrInvalidPath)
	}
	if err := s.Mkdir(ctx, "/nested/without_parent"); !errors.Is(err, domain.ErrParentNotFound) {
		t.Fatalf("Mkdir missing parent: got %v want %v", err, domain.ErrParentNotFound)
	}

	if err := s.Mkdir(ctx, "/a"); err != nil {
		t.Fatal(err)
	}
	if err := s.Mkdir(ctx, "/a/b"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Delete(ctx, "/a"); !errors.Is(err, domain.ErrNotEmpty) {
		t.Fatalf("Delete non-empty dir: got %v want %v", err, domain.ErrNotEmpty)
	}

	if _, err := s.Delete(ctx, "/"); !errors.Is(err, domain.ErrInvalidPath) {
		t.Fatalf("Delete root: got %v want %v", err, domain.ErrInvalidPath)
	}

	if err := s.Rename(ctx, "/missing", "/x"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("Rename missing: got %v want %v", err, domain.ErrNotFound)
	}
}
