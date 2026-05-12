package main

import (
	"context"
	"math"

	"golang.org/x/time/rate"

	"godfs/internal/domain"
	"godfs/internal/observability"
)

// wrapMaintChecksumVerifier applies an optional global rate limit on ChecksumChunk traffic
// from master background maintenance and records RPC outcome metrics.
func wrapMaintChecksumVerifier(inner func(ctx context.Context, addr string, chunkID domain.ChunkID) ([]byte, error), maxQPS float64) func(ctx context.Context, addr string, chunkID domain.ChunkID) ([]byte, error) {
	var lim *rate.Limiter
	if maxQPS > 0 {
		b := int(math.Ceil(maxQPS))
		if b < 1 {
			b = 1
		}
		if b > 64 {
			b = 64
		}
		lim = rate.NewLimiter(rate.Limit(maxQPS), b)
	}
	return func(ctx context.Context, addr string, chunkID domain.ChunkID) ([]byte, error) {
		if lim != nil {
			if err := lim.Wait(ctx); err != nil {
				return nil, err
			}
		}
		sum, err := inner(ctx, addr, chunkID)
		observability.RecordMaintChecksumRPC(err == nil)
		return sum, err
	}
}
