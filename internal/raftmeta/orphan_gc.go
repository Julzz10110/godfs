package raftmeta

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	godfsv1 "godfs/api/proto/godfs/v1"
	"godfs/internal/domain"
)

// OrphanGCOnce removes chunks present on chunkservers but absent in metadata.
// Best-effort: skips nodes that cannot be reached.
func (s *Service) OrphanGCOnce(ctx context.Context) error {
	known := s.SnapshotChunkIDs()
	nodes := s.SnapshotNodes()

	for _, n := range nodes {
		cc, err := grpc.NewClient(n.GRPCAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}
		cli := godfsv1.NewChunkServiceClient(cc)
		lr, err := cli.ListChunks(ctx, &godfsv1.ListChunksRequest{})
		_ = cc.Close()
		if err != nil {
			continue
		}
		for _, id := range lr.ChunkIds {
			cid := domain.ChunkID(id)
			if _, ok := known[cid]; ok {
				continue
			}
			cc2, err := grpc.NewClient(n.GRPCAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				continue
			}
			_, _ = godfsv1.NewChunkServiceClient(cc2).DeleteChunk(ctx, &godfsv1.DeleteChunkRequest{ChunkId: id})
			_ = cc2.Close()
		}
	}
	return nil
}

