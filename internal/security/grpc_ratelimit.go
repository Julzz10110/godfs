package security

import (
	"context"
	"os"
	"strconv"
	"strings"

	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GRPCUnaryRateLimitFromEnv returns a unary server interceptor when global and/or per-peer limits are configured.
//
// Global: GODFS_GRPC_RATE_LIMIT_RPS / GODFS_GRPC_RATE_LIMIT_BURST (process-wide bucket).
// Per-peer: GODFS_GRPC_PEER_RATE_LIMIT_RPS / GODFS_GRPC_PEER_RATE_LIMIT_BURST (key = mTLS CN or Bearer hash).
//
// Master: RegisterNode and Heartbeat are exempt.
func GRPCUnaryRateLimitFromEnv() grpc.UnaryServerInterceptor {
	rps, burst := parseGRPCRateLimitEnv()
	var global *rate.Limiter
	if rps > 0 && burst > 0 {
		global = rate.NewLimiter(rate.Limit(rps), burst)
	}
	peerLim := newPeerRateLimiterFromEnv()
	if global == nil && peerLim == nil {
		return nil
	}
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if exemptFromGRPCUnaryRateLimit(info.FullMethod) {
			return handler(ctx, req)
		}
		if global != nil && !global.Allow() {
			return nil, status.Error(codes.ResourceExhausted, "grpc rate limit exceeded")
		}
		if peerLim != nil && !peerLim.allow(grpcRateLimitPeerKey(ctx)) {
			return nil, status.Error(codes.ResourceExhausted, "grpc peer rate limit exceeded")
		}
		return handler(ctx, req)
	}
}

func exemptFromGRPCUnaryRateLimit(fullMethod string) bool {
	switch fullMethod {
	case "/godfs.v1.MasterService/RegisterNode", "/godfs.v1.MasterService/Heartbeat":
		return true
	default:
		return false
	}
}

func parseGRPCRateLimitEnv() (rps float64, burst int) {
	v := strings.TrimSpace(os.Getenv("GODFS_GRPC_RATE_LIMIT_RPS"))
	if v == "" {
		return 0, 0
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f <= 0 {
		return 0, 0
	}
	rps = f
	if s := strings.TrimSpace(os.Getenv("GODFS_GRPC_RATE_LIMIT_BURST")); s != "" {
		if b, err := strconv.Atoi(s); err == nil && b > 0 {
			return rps, b
		}
	}
	burst = int(2.0 * rps)
	if burst < 10 {
		burst = 10
	}
	return rps, burst
}
