package metadata

import (
	"context"
	"time"

	"godfs/internal/dataplane"
	"godfs/internal/domain"
)

// ListUnderReplicatedChunks returns chunks with fewer live replicas than the replication factor.
func (s *Store) ListUnderReplicatedChunks(ctx context.Context, limit int) ([]domain.UnderReplicatedChunk, int, error) {
	_ = ctx
	s.mu.RLock()
	defer s.mu.RUnlock()

	at := time.Now().UTC()
	isAlive := func(id domain.NodeID) bool {
		if s.nodeDeadAfter <= 0 {
			return true
		}
		return s.isAliveAt(id, at)
	}

	pathByChunk := make(map[domain.ChunkID][]string)
	for p, fr := range s.files {
		if fr == nil {
			continue
		}
		for _, cid := range fr.chunks {
			if cid == "" {
				continue
			}
			pathByChunk[cid] = append(pathByChunk[cid], p)
		}
	}

	views := make(map[domain.ChunkID][]dataplane.ChunkReplicaView, len(s.chunks))
	for id, cr := range s.chunks {
		if cr == nil {
			continue
		}
		reps := make([]dataplane.ChunkReplicaView, len(cr.replicas))
		for i, r := range cr.replicas {
			reps[i] = dataplane.ChunkReplicaView{NodeID: r.NodeID}
		}
		views[id] = reps
	}

	entries, total := dataplane.ListUnderReplicated(s.replicationFactor, isAlive, views, pathByChunk, limit)
	return entries, total, nil
}
