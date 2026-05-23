package metadata

import (
	"context"
	"fmt"
	"testing"
	"time"

	"godfs/internal/domain"
)

func TestStore_ListUnderReplicatedChunks_emptyWhenHealthy(t *testing.T) {
	s := NewStore(1024, 3)
	s.nodeDeadAfter = 30 * time.Second
	ctx := context.Background()
	for i, addr := range []string{"127.0.0.1:1", "127.0.0.1:2", "127.0.0.1:3"} {
		id := domain.NodeID(fmt.Sprintf("n%d", i))
		if err := s.RegisterNode(ctx, domain.ChunkNode{ID: id, GRPCAddress: addr, CapacityBytes: 1 << 30}); err != nil {
			t.Fatal(err)
		}
		if err := s.Heartbeat(ctx, id, 1<<30, 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Mkdir(ctx, "/d"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateFile(ctx, "/d/f.txt"); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, _, _, _, _, _, err := s.PrepareWrite(ctx, "/d/f.txt", 0, 10); err != nil {
		t.Fatal(err)
	}
	entries, total, err := s.ListUnderReplicatedChunks(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 0 || len(entries) != 0 {
		t.Fatalf("expected empty, got total=%d entries=%+v", total, entries)
	}
}

func TestStore_ListUnderReplicatedChunks_deadNode(t *testing.T) {
	s := NewStore(1024, 3)
	s.nodeDeadAfter = 1 * time.Second
	ctx := context.Background()
	for i, addr := range []string{"127.0.0.1:1", "127.0.0.1:2", "127.0.0.1:3"} {
		id := domain.NodeID(fmt.Sprintf("n%d", i))
		if err := s.RegisterNode(ctx, domain.ChunkNode{ID: id, GRPCAddress: addr, CapacityBytes: 1 << 30}); err != nil {
			t.Fatal(err)
		}
		if err := s.Heartbeat(ctx, id, 1<<30, 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Mkdir(ctx, "/d"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateFile(ctx, "/d/f.txt"); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, _, _, _, _, _, err := s.PrepareWrite(ctx, "/d/f.txt", 0, 10); err != nil {
		t.Fatal(err)
	}
	// Stop heartbeats on n2; wait until considered dead.
	time.Sleep(1100 * time.Millisecond)
	entries, total, err := s.ListUnderReplicatedChunks(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total < 1 || len(entries) < 1 {
		t.Fatalf("expected under-replicated chunks, total=%d entries=%+v", total, entries)
	}
	if entries[0].AliveReplicas >= 3 {
		t.Fatalf("expected alive < 3, got %+v", entries[0])
	}
}
