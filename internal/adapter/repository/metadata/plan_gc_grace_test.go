package metadata

import (
	"testing"
	"time"

	"godfs/internal/domain"
)

func TestPlanDeleteGC_PendingDeleteGrace(t *testing.T) {
	s := NewStore(4096, 2)
	s.SetGCPendingDeleteGrace(30 * time.Minute)

	now := time.Date(2026, 1, 2, 15, 0, 0, 0, time.UTC)
	created := now.Add(-5 * time.Minute).Unix()

	s.mu.Lock()
	cid := domain.ChunkID("chk1")
	s.pendingDeletes[cid] = map[string]*pendingChunkDelete{
		"127.0.0.1:9000": {CreatedUnix: created, Attempts: 0, NextAttemptUnix: 0},
	}
	s.mu.Unlock()

	_, _, _, ok := s.PlanDeleteGC(now)
	if ok {
		t.Fatal("expected no delete while inside grace window")
	}

	later := now.Add(31 * time.Minute)
	c2, addr, attempts, ok2 := s.PlanDeleteGC(later)
	if !ok2 {
		t.Fatal("expected due after grace")
	}
	if c2 != cid || addr != "127.0.0.1:9000" || attempts != 0 {
		t.Fatalf("unexpected pick: %s %s %d", c2, addr, attempts)
	}
}
