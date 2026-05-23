package dataplane

import (
	"testing"

	"godfs/internal/domain"
)

func TestListUnderReplicated(t *testing.T) {
	chunks := map[domain.ChunkID][]ChunkReplicaView{
		"c1": {{NodeID: "a"}, {NodeID: "b"}, {NodeID: "c"}},
		"c2": {{NodeID: "a"}, {NodeID: "b"}},
	}
	isAlive := func(id domain.NodeID) bool { return id != "b" }
	paths := map[domain.ChunkID][]string{"c1": {"/x"}}
	entries, total := ListUnderReplicated(3, isAlive, chunks, paths, 0)
	if total != 2 || len(entries) != 2 {
		t.Fatalf("total=%d len=%d", total, len(entries))
	}
	if entries[0].ChunkID != "c1" || entries[0].AliveReplicas != 2 {
		t.Fatalf("c1: %+v", entries[0])
	}
}
