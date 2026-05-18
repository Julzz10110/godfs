package metadata

import (
	"testing"
	"time"

	"godfs/internal/domain"
)

func TestGCStrict_PendingCountedAtMaxAttempts(t *testing.T) {
	s := NewStore(1024, 1)
	cid := domain.ChunkID("gc-strict-1")
	s.mu.Lock()
	s.pendingDeletes[cid] = map[string]*pendingChunkDelete{
		"127.0.0.1:9001": {Attempts: 5, CreatedUnix: time.Now().Unix()},
	}
	s.mu.Unlock()
	if n := s.CountGCDeleteEntriesAtMaxAttempts(5); n != 1 {
		t.Fatalf("got %d want 1", n)
	}
}
