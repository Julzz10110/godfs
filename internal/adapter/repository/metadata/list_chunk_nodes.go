package metadata

import (
	"context"
	"time"

	"godfs/internal/domain"
)

// ListChunkNodes returns registered chunk nodes and best-effort liveness.
func (s *Store) ListChunkNodes(ctx context.Context) ([]domain.ChunkNodeDiag, error) {
	_ = ctx
	s.mu.RLock()
	defer s.mu.RUnlock()

	at := time.Now().UTC()
	out := make([]domain.ChunkNodeDiag, 0, len(s.nodes))
	for _, n := range s.nodes {
		e := domain.ChunkNodeDiag{
			ID:            n.ID,
			GRPCAddress:   n.GRPCAddress,
			CapacityBytes: n.CapacityBytes,
		}
		if st, ok := s.nodeStatus[n.ID]; ok {
			e.UsedBytes = st.UsedBytes
			if !st.LastSeen.IsZero() {
				e.LastSeenUnix = st.LastSeen.Unix()
			}
			if s.nodeDeadAfter <= 0 {
				e.Alive = true
			} else {
				e.Alive = s.isAliveAt(n.ID, at)
			}
		}
		out = append(out, e)
	}
	return out, nil
}
