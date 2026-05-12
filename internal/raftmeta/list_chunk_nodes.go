package raftmeta

import (
	"context"
	"time"

	"godfs/internal/domain"
)

// ListChunkNodes returns registered chunk nodes and best-effort liveness from local FSM state.
func (s *Service) ListChunkNodes(ctx context.Context) ([]domain.ChunkNodeDiag, error) {
	_ = ctx
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	st := s.fsm.st
	if st == nil {
		return nil, nil
	}
	at := time.Now().UTC()
	out := make([]domain.ChunkNodeDiag, 0, len(st.Nodes))
	for _, n := range st.Nodes {
		e := domain.ChunkNodeDiag{
			ID:            n.ID,
			GRPCAddress:   n.GRPCAddress,
			CapacityBytes: n.CapacityBytes,
		}
		if hb, ok := st.NodeStatus[n.ID]; ok {
			e.UsedBytes = hb.UsedBytes
			if !hb.LastSeen.IsZero() {
				e.LastSeenUnix = hb.LastSeen.Unix()
			}
			if st.NodeDeadAfter <= 0 {
				e.Alive = true
			} else {
				e.Alive = st.isAliveAt(n.ID, at)
			}
		}
		out = append(out, e)
	}
	return out, nil
}
