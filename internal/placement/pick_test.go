package placement

import (
	"errors"
	"testing"

	"godfs/internal/domain"
)

func TestPick_PrefersMoreFreeSpace(t *testing.T) {
	nodes := []domain.ChunkNode{
		{ID: "a", GRPCAddress: "127.0.0.1:1", CapacityBytes: 100},
		{ID: "b", GRPCAddress: "127.0.0.1:2", CapacityBytes: 100},
		{ID: "c", GRPCAddress: "127.0.0.1:3", CapacityBytes: 100},
	}
	used := map[domain.NodeID]int64{
		"a": 80,
		"b": 10,
		"c": 50,
	}
	out, err := Pick(nodes, 2, used)
	if err != nil {
		t.Fatal(err)
	}
	// Most free: b (90), then c (50), then a (20)
	if out[0].ID != "b" || out[1].ID != "c" {
		t.Fatalf("got %+v", out)
	}
}

func TestPick_InsufficientNodes(t *testing.T) {
	nodes := []domain.ChunkNode{{ID: "a", CapacityBytes: 100}}
	_, err := Pick(nodes, 3, map[domain.NodeID]int64{})
	if !errors.Is(err, domain.ErrInsufficientChunkServers) {
		t.Fatalf("want ErrInsufficientChunkServers, got %v", err)
	}
}
