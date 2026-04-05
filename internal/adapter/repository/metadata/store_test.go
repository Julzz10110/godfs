package metadata

import (
	"context"
	"testing"

	"godfs/internal/domain"
)

func TestStoreMkdirCreateWrite(t *testing.T) {
	s := NewStore(1024, 1)
	ctx := context.Background()

	if err := s.RegisterNode(ctx, domain.ChunkNode{ID: "n1", GRPCAddress: "127.0.0.1:9"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Mkdir(ctx, "/data"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateFile(ctx, "/data/a.txt"); err != nil {
		t.Fatal(err)
	}

	cid, addr, _, _, idx, off, _, ver, err := s.PrepareWrite(ctx, "/data/a.txt", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if cid == "" || addr == "" || idx != 0 || off != 0 || ver != 1 {
		t.Fatalf("prepare: cid=%s addr=%s idx=%d off=%d ver=%d", cid, addr, idx, off, ver)
	}
	if err := s.CommitChunk(ctx, "/data/a.txt", cid, idx, off, 10, []byte("checksum"), ver); err != nil {
		t.Fatal(err)
	}

	isDir, sz, _, _, _, err := s.Stat(ctx, "/data/a.txt")
	if err != nil || isDir || sz != 10 {
		t.Fatalf("stat: isDir=%v sz=%d err=%v", isDir, sz, err)
	}
}
