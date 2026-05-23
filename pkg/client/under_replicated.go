package client

import (
	"context"

	godfsv1 "godfs/api/proto/godfs/v1"
)

// ListUnderReplicatedChunks returns chunks below target replication (admin-only RPC).
func (c *Client) ListUnderReplicatedChunks(ctx context.Context, limit int32) ([]*godfsv1.UnderReplicatedChunkEntry, int32, error) {
	r, err := c.master.ListUnderReplicatedChunks(ctx, &godfsv1.ListUnderReplicatedChunksRequest{Limit: limit})
	if err != nil {
		return nil, 0, err
	}
	return r.GetChunks(), r.GetTotalCount(), nil
}
