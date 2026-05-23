package metadata

import (
	"context"
	"testing"

	"godfs/internal/domain"
)

func TestStore_TruncateFile_shrinkAndExtend(t *testing.T) {
	s := NewStore(100, 1)
	ctx := context.Background()
	if err := s.Mkdir(ctx, "/d"); err != nil {
		t.Fatal(err)
	}
	if err := s.RegisterNode(ctx, domain.ChunkNode{ID: "n1", GRPCAddress: "127.0.0.1:1", CapacityBytes: 1 << 30}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateFile(ctx, "/d/f.txt"); err != nil {
		t.Fatal(err)
	}
	cid, _, _, _, _, idx, off, _, ver, err := s.PrepareWrite(ctx, "/d/f.txt", 0, 50)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CommitChunk(ctx, "/d/f.txt", cid, idx, off, 50, nil, ver); err != nil {
		t.Fatal(err)
	}
	// extend via truncate (sparse)
	if _, err := s.TruncateFile(ctx, "/d/f.txt", 200); err != nil {
		t.Fatal(err)
	}
	_, sz, _, _, _, err := s.Stat(ctx, "/d/f.txt")
	if err != nil || sz != 200 {
		t.Fatalf("size=%d err=%v", sz, err)
	}
	// shrink to zero
	infos, err := s.TruncateFile(ctx, "/d/f.txt", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 1 {
		t.Fatalf("expected 1 chunk removed, got %d", len(infos))
	}
	_, sz, _, _, _, err = s.Stat(ctx, "/d/f.txt")
	if err != nil || sz != 0 {
		t.Fatalf("size=%d err=%v", sz, err)
	}
}

func TestStore_TruncateFile_sparseRead(t *testing.T) {
	s := NewStore(64, 1)
	ctx := context.Background()
	if err := s.Mkdir(ctx, "/d"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateFile(ctx, "/d/hole.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.TruncateFile(ctx, "/d/hole.txt", 32); err != nil {
		t.Fatal(err)
	}
	_, _, _, avail, _, _, err := s.GetChunkForRead(ctx, "/d/hole.txt", 0)
	if err != nil {
		t.Fatal(err)
	}
	if avail != 32 {
		t.Fatalf("avail=%d want 32", avail)
	}
}

func TestStore_TruncateFile_clearsPartialChunkChecksum(t *testing.T) {
	s := NewStore(64, 1)
	ctx := context.Background()
	if err := s.Mkdir(ctx, "/d"); err != nil {
		t.Fatal(err)
	}
	if err := s.RegisterNode(ctx, domain.ChunkNode{ID: "n1", GRPCAddress: "127.0.0.1:1", CapacityBytes: 1 << 30}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateFile(ctx, "/d/f.txt"); err != nil {
		t.Fatal(err)
	}
	cid, _, _, _, _, idx, off, _, ver, err := s.PrepareWrite(ctx, "/d/f.txt", 0, 20)
	if err != nil {
		t.Fatal(err)
	}
	sum := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
	if err := s.CommitChunk(ctx, "/d/f.txt", cid, idx, off, 20, sum, ver); err != nil {
		t.Fatal(err)
	}
	if _, err := s.TruncateFile(ctx, "/d/f.txt", 2); err != nil {
		t.Fatal(err)
	}
	_, _, _, _, _, chk, err := s.GetChunkForRead(ctx, "/d/f.txt", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(chk) != 0 {
		t.Fatalf("expected checksum cleared after partial truncate, got len=%d", len(chk))
	}
}
