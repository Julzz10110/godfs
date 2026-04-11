package raftmeta

import (
	"context"
	"time"

	"google.golang.org/grpc"

	godfsv1 "godfs/api/proto/godfs/v1"
	"godfs/internal/domain"
	"godfs/internal/security"
)

// OrphanGCOnce removes chunks present on chunkservers but absent in metadata.
// Best-effort: skips nodes that cannot be reached.
func (s *Service) OrphanGCOnce(ctx context.Context, minAge time.Duration, maxDeletesPerNode int) error {
	known := s.SnapshotChunkIDs()
	nodes := s.SnapshotNodes()
	now := time.Now().UTC()

	for _, n := range nodes {
		// Skip nodes considered dead by heartbeat.
		s.fsm.mu.RLock()
		deadAfter := s.fsm.st.NodeDeadAfter
		alive := true
		if deadAfter > 0 {
			alive = s.fsm.st.isAliveAt(n.ID, now)
		}
		s.fsm.mu.RUnlock()
		if !alive {
			continue
		}
		dopts, err := security.ClientDialOptions()
		if err != nil {
			continue
		}
		cc, err := grpc.NewClient(n.GRPCAddress, dopts...)
		if err != nil {
			continue
		}
		cli := godfsv1.NewChunkServiceClient(cc)
		lr, err := cli.ListChunks(ctx, &godfsv1.ListChunksRequest{})
		_ = cc.Close()
		if err != nil {
			continue
		}
		deleted := 0
		for _, info := range lr.Chunks {
			id := info.ChunkId
			cid := domain.ChunkID(id)
			if _, ok := known[cid]; ok {
				continue
			}
			if minAge > 0 {
				mt := time.Unix(info.ModifiedAtUnix, 0).UTC()
				if now.Sub(mt) < minAge {
					continue
				}
			}
			if maxDeletesPerNode > 0 && deleted >= maxDeletesPerNode {
				break
			}
			cc2, err := grpc.NewClient(n.GRPCAddress, dopts...)
			if err != nil {
				continue
			}
			_, _ = godfsv1.NewChunkServiceClient(cc2).DeleteChunk(ctx, &godfsv1.DeleteChunkRequest{ChunkId: id})
			_ = cc2.Close()
			deleted++
		}
	}
	return nil
}

