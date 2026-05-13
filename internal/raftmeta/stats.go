package raftmeta

import (
	"strings"
	"time"

	"godfs/internal/domain"
)

// DataPlaneStats is a best-effort summary derived from Raft metadata state.
// It intentionally avoids network calls.
type DataPlaneStats struct {
	UnderReplicatedChunks int
	PendingDeletes        int
	UnrepairableChunks    int

	RebalanceQueueDepth int
	GCQueuedChunks      int

	ChunkNodesAlive int
	ChunkNodesDead  int
}

func (s *Service) DataPlaneStats(at time.Time) DataPlaneStats {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	st := DataPlaneStats{}
	if s.fsm.st == nil {
		return st
	}
	state := s.fsm.st

	targetRF := state.ReplicationFactor
	isAlive := func(id domain.NodeID) bool {
		if state.NodeDeadAfter <= 0 {
			return true
		}
		return state.isAliveAt(id, at)
	}

	if targetRF > 1 {
		for _, cr := range state.Chunks {
			if cr == nil {
				continue
			}
			alive := 0
			for _, r := range cr.Replicas {
				if isAlive(r.NodeID) {
					alive++
				}
			}
			if alive < targetRF {
				st.UnderReplicatedChunks++
			}
		}
	}
	for _, addrs := range state.PendingDeletes {
		st.PendingDeletes += len(addrs)
	}
	st.RebalanceQueueDepth = len(state.RebalanceTasks)
	st.GCQueuedChunks = len(state.PendingDeletes)
	if state.NodeDeadAfter > 0 {
		for id := range state.NodeStatus {
			if state.isAliveAt(id, at) {
				st.ChunkNodesAlive++
			} else {
				st.ChunkNodesDead++
			}
		}
	} else {
		st.ChunkNodesAlive = len(state.NodeStatus)
	}
	for _, t := range state.RebalanceTasks {
		if t == nil {
			continue
		}
		if strings.HasPrefix(t.LastError, "unrepairable:") {
			st.UnrepairableChunks++
		}
	}
	return st
}

