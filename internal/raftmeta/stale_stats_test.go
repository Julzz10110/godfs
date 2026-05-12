package raftmeta

import (
	"bytes"
	"context"
	"testing"

	"godfs/internal/domain"
)

func TestCountStaleReplicasOneChunk(t *testing.T) {
	ctx := context.Background()
	meta := bytes.Repeat([]byte{7}, 32)
	reps := []domain.ChunkReplica{
		{NodeID: "a", Address: "addr-good"},
		{NodeID: "b", Address: "addr-bad"},
	}
	isAlive := func(domain.NodeID) bool { return true }

	rep := func(_ context.Context, addr string, _ domain.ChunkID) ([]byte, error) {
		switch addr {
		case "addr-good":
			return append([]byte(nil), meta...), nil
		case "addr-bad":
			wrong := append([]byte(nil), meta...)
			wrong[0] ^= 0xff
			return wrong, nil
		default:
			return nil, context.Canceled
		}
	}

	n := countStaleReplicasOneChunk(ctx, "c1", meta, reps, isAlive, rep)
	if n != 1 {
		t.Fatalf("stale count: got %d want 1", n)
	}

	repMatch := func(_ context.Context, _ string, _ domain.ChunkID) ([]byte, error) {
		return append([]byte(nil), meta...), nil
	}
	if m := countStaleReplicasOneChunk(ctx, "c2", meta, reps, isAlive, repMatch); m != 0 {
		t.Fatalf("want 0 stale, got %d", m)
	}
}
