package metadata

import (
	"context"
	"testing"
	"time"
)

func TestSoftDelete_RestoreWithinGrace(t *testing.T) {
	s := NewStore(1024, 1)
	s.SetSoftDeleteGrace(time.Hour)
	ctx := context.Background()

	_ = s.Mkdir(ctx, "/t")
	_, _ = s.CreateFile(ctx, "/t/f")
	_, err := s.Delete(ctx, "/t/f")
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, _, _, err = s.Stat(ctx, "/t/f")
	if err == nil {
		t.Fatal("expected not found while in trash")
	}
	if err := s.RestoreFile(ctx, "/t/f"); err != nil {
		t.Fatal(err)
	}
	_, _, _, _, _, err = s.Stat(ctx, "/t/f")
	if err != nil {
		t.Fatalf("stat after restore: %v", err)
	}
}

func TestSoftDelete_PurgeAfterGrace(t *testing.T) {
	s := NewStore(1024, 1)
	s.SetSoftDeleteGrace(10 * time.Millisecond)
	ctx := context.Background()
	_ = s.Mkdir(ctx, "/t")
	_, _ = s.CreateFile(ctx, "/t/g")
	_, _ = s.Delete(ctx, "/t/g")
	time.Sleep(15 * time.Millisecond)
	s.PurgeExpiredSoftDeletes(time.Now().UTC())
	_, _, _, _, _, err := s.Stat(ctx, "/t/g")
	if err == nil {
		t.Fatal("expected purged file gone")
	}
	if err := s.RestoreFile(ctx, "/t/g"); err == nil {
		t.Fatal("restore after purge should fail")
	}
}
