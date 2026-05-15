package security

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"
)

func TestGRPCRateLimitPeerKey_BearerDistinct(t *testing.T) {
	ctx1 := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer token-a"))
	ctx2 := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer token-b"))
	k1 := grpcRateLimitPeerKey(ctx1)
	k2 := grpcRateLimitPeerKey(ctx2)
	if k1 == k2 || k1 == "" || k2 == "" {
		t.Fatalf("keys should differ: %q %q", k1, k2)
	}
}

func TestPeerRateLimiter_IndependentBuckets(t *testing.T) {
	t.Setenv("GODFS_GRPC_PEER_RATE_LIMIT_RPS", "1")
	t.Setenv("GODFS_GRPC_PEER_RATE_LIMIT_BURST", "1")
	pl := newPeerRateLimiterFromEnv()
	if pl == nil {
		t.Fatal("expected peer limiter")
	}
	if !pl.allow("peer-a") {
		t.Fatal("first allow peer-a")
	}
	if pl.allow("peer-a") {
		t.Fatal("second allow peer-a should fail")
	}
	if !pl.allow("peer-b") {
		t.Fatal("peer-b should have separate bucket")
	}
}

func TestParseGRPCPeerRateLimitEnv_Disabled(t *testing.T) {
	t.Setenv("GODFS_GRPC_PEER_RATE_LIMIT_RPS", "")
	rps, burst := parseGRPCPeerRateLimitEnv()
	if rps != 0 || burst != 0 {
		t.Fatalf("got rps=%v burst=%v", rps, burst)
	}
}
