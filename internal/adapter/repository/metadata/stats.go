package metadata

import (
	"strings"
	"time"

	"godfs/internal/domain"
)

// DataPlaneStats is a best-effort summary of data-plane health derived from metadata.
// It intentionally avoids network calls.
type DataPlaneStats struct {
	UnderReplicatedChunks int
	PendingDeletes        int
	UnrepairableChunks    int

	// RebalanceQueueDepth is the number of chunk IDs with in-flight rebalance state machines.
	RebalanceQueueDepth int
	// GCQueuedChunks is the number of chunk IDs that have at least one pending DeleteChunk entry.
	GCQueuedChunks int

	ChunkNodesAlive int
	ChunkNodesDead  int
}

func (s *Store) DataPlaneStats(at time.Time) DataPlaneStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	st := DataPlaneStats{}

	targetRF := s.replicationFactor
	deadAfter := s.nodeDeadAfter
	isAlive := func(id domain.NodeID) bool {
		if deadAfter <= 0 {
			return true
		}
		return s.isAliveAt(id, at)
	}

	if targetRF > 1 {
		for _, cr := range s.chunks {
			if cr == nil {
				continue
			}
			alive := 0
			for _, r := range cr.replicas {
				if isAlive(r.NodeID) {
					alive++
				}
			}
			if alive < targetRF {
				st.UnderReplicatedChunks++
			}
		}
	}

	for _, addrs := range s.pendingDeletes {
		st.PendingDeletes += len(addrs)
	}
	st.RebalanceQueueDepth = len(s.rebalanceTasks)
	st.GCQueuedChunks = len(s.pendingDeletes)

	// Chunk node liveness summary.
	if s.nodeDeadAfter > 0 {
		for id := range s.nodeStatus {
			if s.isAliveAt(id, at) {
				st.ChunkNodesAlive++
			} else {
				st.ChunkNodesDead++
			}
		}
	} else {
		// If liveness filtering disabled, treat all known nodes as alive.
		st.ChunkNodesAlive = len(s.nodeStatus)
	}

	for _, t := range s.rebalanceTasks {
		if t == nil {
			continue
		}
		if strings.HasPrefix(t.LastError, "unrepairable:") {
			st.UnrepairableChunks++
		}
	}
	return st
}
