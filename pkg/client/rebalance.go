package client

import (
	"context"

	godfsv1 "godfs/api/proto/godfs/v1"
)

// RunRebalanceNow asks the leader to execute up to maxSteps rebalance iterations (admin-only).
func (c *Client) RunRebalanceNow(ctx context.Context, maxSteps int32) (executed int32, err error) {
	r, err := c.master.RunRebalanceNow(ctx, &godfsv1.RunRebalanceNowRequest{MaxSteps: maxSteps})
	if err != nil {
		return 0, err
	}
	return r.GetExecuted(), nil
}
