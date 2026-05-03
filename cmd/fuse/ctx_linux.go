//go:build linux

package main

import "context"

func (n *node) opCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	if n == nil || n.rpcTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, n.rpcTimeout)
}
