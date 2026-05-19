package metadata

import (
	"context"
	"testing"

	"godfs/internal/domain"
)

func TestStoreRestoreSnapshot_RoundTrip(t *testing.T) {
	ctx := context.Background()

	s := NewStore(1024, 2)
	_ = s.RegisterNode(ctx, domain.ChunkNode{ID: "n1", GRPCAddress: "127.0.0.1:10001", CapacityBytes: 1 << 30})
	_ = s.RegisterNode(ctx, domain.ChunkNode{ID: "n2", GRPCAddress: "127.0.0.1:10002", CapacityBytes: 1 << 30})

	if err := s.Mkdir(ctx, "/a"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if _, err := s.CreateFile(ctx, "/a/f.txt"); err != nil {
		t.Fatalf("create file: %v", err)
	}
	cid, _, _, _, _, _, _, _, ver, err := s.PrepareWrite(ctx, "/a/f.txt", 0, 5)
	if err != nil {
		t.Fatalf("prepare write: %v", err)
	}
	if err := s.CommitChunk(ctx, "/a/f.txt", cid, 0, 0, 5, []byte("sum"), ver); err != nil {
		t.Fatalf("commit: %v", err)
	}

	snapID, _, err := s.CreateSnapshot(ctx, "dr-test")
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}
	man, err := s.GetSnapshot(ctx, snapID)
	if err != nil {
		t.Fatalf("get snapshot: %v", err)
	}

	s2 := NewStore(1, 1)
	if err := s2.RestoreSnapshot(ctx, man, true); err != nil {
		t.Fatalf("restore: %v", err)
	}

	isDir, size, _, _, _, err := s2.Stat(ctx, "/a/f.txt")
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if isDir {
		t.Fatalf("expected file, got dir")
	}
	if size != 5 {
		t.Fatalf("expected size=5, got %d", size)
	}

	rcid, reps, off, avail, rver, sum, err := s2.GetChunkForRead(ctx, "/a/f.txt", 0)
	if err != nil {
		t.Fatalf("get chunk: %v", err)
	}
	if rcid != cid {
		t.Fatalf("expected chunkID=%s, got %s", cid, rcid)
	}
	if off != 0 || avail != 5 {
		t.Fatalf("unexpected off/avail: %d/%d", off, avail)
	}
	if rver == 0 {
		t.Fatalf("expected version > 0")
	}
	if string(sum) != "sum" {
		t.Fatalf("unexpected checksum: %q", string(sum))
	}
	if len(reps) == 0 {
		t.Fatalf("expected replicas")
	}
}
