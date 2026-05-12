package client

import (
	"context"

	godfsv1 "godfs/api/proto/godfs/v1"
)

// ListChunkNodes returns registered chunk nodes and best-effort liveness (admin-only RPC).
func (c *Client) ListChunkNodes(ctx context.Context) ([]*godfsv1.ChunkNodeEntry, error) {
	var r *godfsv1.ListChunkNodesResponse
	err := grpcRetry(ctx, 3, func() error {
		var e error
		r, e = c.master.ListChunkNodes(ctx, &godfsv1.ListChunkNodesRequest{})
		return e
	})
	if err != nil {
		return nil, err
	}
	return r.GetNodes(), nil
}
