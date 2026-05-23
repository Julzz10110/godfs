package raftmeta

import (
	"context"
	"time"

	"godfs/internal/dataplane"
	"godfs/internal/domain"
)

// ListUnderReplicatedChunks returns chunks with fewer live replicas than the replication factor.
func (s *Service) ListUnderReplicatedChunks(ctx context.Context, limit int) ([]domain.UnderReplicatedChunk, int, error) {
	_ = ctx
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	st := s.fsm.st
	if st == nil {
		return nil, 0, nil
	}
	at := time.Now().UTC()
	isAlive := func(id domain.NodeID) bool {
		if st.NodeDeadAfter <= 0 {
			return true
		}
		return st.isAliveAt(id, at)
	}

	pathByChunk := make(map[domain.ChunkID][]string)
	for p, fr := range st.Files {
		if fr == nil {
			continue
		}
		for _, cid := range fr.Chunks {
			if cid == "" {
				continue
			}
			pathByChunk[cid] = append(pathByChunk[cid], p)
		}
	}

	views := make(map[domain.ChunkID][]dataplane.ChunkReplicaView, len(st.Chunks))
	for id, cr := range st.Chunks {
		if cr == nil {
			continue
		}
		reps := make([]dataplane.ChunkReplicaView, len(cr.Replicas))
		for i, r := range cr.Replicas {
			reps[i] = dataplane.ChunkReplicaView{NodeID: r.NodeID}
		}
		views[id] = reps
	}

	entries, total := dataplane.ListUnderReplicated(st.ReplicationFactor, isAlive, views, pathByChunk, limit)
	return entries, total, nil
}
