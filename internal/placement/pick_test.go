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
	out, err := Pick(nodes, 2, used, 0)
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
	_, err := Pick(nodes, 3, map[domain.NodeID]int64{}, 0)
	if !errors.Is(err, domain.ErrInsufficientChunkServers) {
		t.Fatalf("want ErrInsufficientChunkServers, got %v", err)
	}
}

func TestPick_RoundRobinTieBreak(t *testing.T) {
	nodes := []domain.ChunkNode{
		{ID: "a", CapacityBytes: 100},
		{ID: "b", CapacityBytes: 100},
		{ID: "c", CapacityBytes: 100},
	}
	used := map[domain.NodeID]int64{"a": 0, "b": 0, "c": 0}

	out0, err := Pick(nodes, 1, used, 0)
	if err != nil {
		t.Fatal(err)
	}
	if out0[0].ID != "a" {
		t.Fatalf("rr=0 want first reg node a, got %s", out0[0].ID)
	}
	out1, err := Pick(nodes, 1, used, 1)
	if err != nil {
		t.Fatal(err)
	}
	if out1[0].ID != "c" {
		t.Fatalf("rr=1 want c, got %s", out1[0].ID)
	}
	out2, err := Pick(nodes, 1, used, 2)
	if err != nil {
		t.Fatal(err)
	}
	if out2[0].ID != "b" {
		t.Fatalf("rr=2 want b, got %s", out2[0].ID)
	}
}
