package raftmeta

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"godfs/internal/domain"
)

func TestStateRestoreSnapshot_RoundTrip(t *testing.T) {
	at := time.Unix(1700000000, 0).UTC()
	st := NewState(1024, 2, 60*time.Second, 0)
	_ = st.RegisterNode(domain.ChunkNode{ID: "n1", GRPCAddress: "127.0.0.1:10001", CapacityBytes: 1 << 30})
	_ = st.RegisterNode(domain.ChunkNode{ID: "n2", GRPCAddress: "127.0.0.1:10002", CapacityBytes: 1 << 30})

	if err := st.Mkdir("/a"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if _, err := st.CreateFile("/a/f.txt", domain.FileID("f1"), at); err != nil {
		t.Fatalf("create file: %v", err)
	}

	lease := domain.LeaseID(uuid.NewString())
	chunkID := domain.ChunkID(uuid.NewString())
	res, err := st.PrepareWrite("/a/f.txt", 0, 5, lease, chunkID, at)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if res.ChunkID != chunkID {
		t.Fatalf("expected chunk id %s, got %s", chunkID, res.ChunkID)
	}
	if err := st.CommitChunk("/a/f.txt", chunkID, 0, 0, 5, []byte("sum"), res.Version, at); err != nil {
		t.Fatalf("commit: %v", err)
	}

	snapID := uuid.NewString()
	if err := st.CreateBackupSnapshot(snapID, "dr-test", at); err != nil {
		t.Fatalf("create snapshot: %v", err)
	}
	man, err := st.GetBackupSnapshot(snapID)
	if err != nil {
		t.Fatalf("get snapshot: %v", err)
	}

	st2 := NewState(1, 1, 60*time.Second, 0)
	if err := st2.RestoreSnapshot(man, true); err != nil {
		t.Fatalf("restore: %v", err)
	}

	isDir, size, _, _, _, err := st2.Stat("/a/f.txt")
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if isDir {
		t.Fatalf("expected file, got dir")
	}
	if size != 5 {
		t.Fatalf("expected size=5, got %d", size)
	}

	rcid, reps, off, avail, _, sum, err := st2.GetChunkForRead("/a/f.txt", 0)
	if err != nil {
		t.Fatalf("get chunk: %v", err)
	}
	if rcid != chunkID {
		t.Fatalf("expected chunkID=%s, got %s", chunkID, rcid)
	}
	if off != 0 || avail != 5 {
		t.Fatalf("unexpected off/avail: %d/%d", off, avail)
	}
	if string(sum) != "sum" {
		t.Fatalf("unexpected checksum: %q", string(sum))
	}
	if len(reps) == 0 {
		t.Fatalf("expected replicas")
	}
}
