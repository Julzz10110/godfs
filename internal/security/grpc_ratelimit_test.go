package security

import (
	"testing"
)

func TestParseGRPCRateLimitEnv_Disabled(t *testing.T) {
	t.Setenv("GODFS_GRPC_RATE_LIMIT_RPS", "")
	t.Setenv("GODFS_GRPC_RATE_LIMIT_BURST", "")
	rps, burst := parseGRPCRateLimitEnv()
	if rps != 0 || burst != 0 {
		t.Fatalf("got rps=%v burst=%v", rps, burst)
	}
}

func TestParseGRPCRateLimitEnv_DefaultBurst(t *testing.T) {
	t.Setenv("GODFS_GRPC_RATE_LIMIT_RPS", "5")
	t.Setenv("GODFS_GRPC_RATE_LIMIT_BURST", "")
	rps, burst := parseGRPCRateLimitEnv()
	if rps != 5 || burst < 10 {
		t.Fatalf("got rps=%v burst=%v", rps, burst)
	}
}

func TestGRPCUnaryRateLimitFromEnv_NilWhenUnset(t *testing.T) {
	t.Setenv("GODFS_GRPC_RATE_LIMIT_RPS", "")
	if GRPCUnaryRateLimitFromEnv() != nil {
		t.Fatal("expected nil interceptor")
	}
}
