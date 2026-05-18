package metadata

import (
	"testing"

	"godfs/internal/domain"
)

func TestCountGCDeleteEntriesAtMaxAttempts(t *testing.T) {
	s := NewStore(1024, 1)
	cid := domain.ChunkID("c-strict")
	s.pendingDeletes[cid] = map[string]*pendingChunkDelete{
		"10.0.0.1:9001": {Attempts: 2},
		"10.0.0.2:9001": {Attempts: 5},
	}
	if n := s.CountGCDeleteEntriesAtMaxAttempts(5); n != 1 {
		t.Fatalf("got %d want 1", n)
	}
	if n := s.CountGCDeleteEntriesAtMaxAttempts(2); n != 2 {
		t.Fatalf("got %d want 2", n)
	}
}
