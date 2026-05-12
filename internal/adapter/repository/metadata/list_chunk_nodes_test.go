package metadata

import (
	"context"
	"testing"
	"time"

	"godfs/internal/domain"
)

func TestStore_ListChunkNodes(t *testing.T) {
	s := NewStore(1024, 1)
	s.nodeDeadAfter = 30 * time.Second
	ctx := context.Background()
	if err := s.RegisterNode(ctx, domain.ChunkNode{ID: "n1", GRPCAddress: "127.0.0.1:1", CapacityBytes: 100}); err != nil {
		t.Fatal(err)
	}
	if err := s.Heartbeat(ctx, "n1", 100, 5); err != nil {
		t.Fatal(err)
	}
	entries, err := s.ListChunkNodes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].ID != "n1" || entries[0].GRPCAddress != "127.0.0.1:1" {
		t.Fatalf("unexpected entries: %+v", entries)
	}
	if !entries[0].Alive || entries[0].UsedBytes != 5 {
		t.Fatalf("unexpected liveness/used: %+v", entries[0])
	}
}
