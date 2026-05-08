package client

import (
	"context"

	godfsv1 "godfs/api/proto/godfs/v1"
)

// ListMasters returns the current Raft master membership (leader-only RPC).
func (c *Client) ListMasters(ctx context.Context) ([]*godfsv1.MasterPeer, string, error) {
	var r *godfsv1.ListMastersResponse
	err := grpcRetry(ctx, 3, func() error {
		var e error
		r, e = c.master.ListMasters(ctx, &godfsv1.ListMastersRequest{})
		return e
	})
	if err != nil {
		return nil, "", err
	}
	return r.GetMasters(), r.GetLeaderNodeId(), nil
}

// AddMaster adds a new Raft master member (leader-only, admin).
func (c *Client) AddMaster(ctx context.Context, nodeID, raftAddr, grpcAddr string) error {
	return grpcRetry(ctx, 3, func() error {
		_, err := c.master.AddMaster(ctx, &godfsv1.AddMasterRequest{
			NodeId:      nodeID,
			RaftAddress: raftAddr,
			GrpcAddress: grpcAddr,
		})
		return err
	})
}

// RemoveMaster removes a Raft master member by node id (leader-only, admin).
func (c *Client) RemoveMaster(ctx context.Context, nodeID string) error {
	return grpcRetry(ctx, 3, func() error {
		_, err := c.master.RemoveMaster(ctx, &godfsv1.RemoveMasterRequest{NodeId: nodeID})
		return err
	})
}

