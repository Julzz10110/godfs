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

// GRPCUnaryRateLimitFromEnv returns a unary server interceptor when GODFS_GRPC_RATE_LIMIT_RPS is set
// to a positive value. It applies a single process-wide token bucket (protects CPU from RPC floods).
//
// Master: RegisterNode and Heartbeat are exempt so chunk clusters are not throttled against the same bucket.
//
// Env:
//   - GODFS_GRPC_RATE_LIMIT_RPS (float64, required to enable)
//   - GODFS_GRPC_RATE_LIMIT_BURST (int, default max(10, ceil(2*RPS)))
func GRPCUnaryRateLimitFromEnv() grpc.UnaryServerInterceptor {
	rps, burst := parseGRPCRateLimitEnv()
	if rps <= 0 || burst <= 0 {
		return nil
	}
	lim := rate.NewLimiter(rate.Limit(rps), burst)
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if exemptFromGRPCUnaryRateLimit(info.FullMethod) {
			return handler(ctx, req)
		}
		if !lim.Allow() {
			return nil, status.Error(codes.ResourceExhausted, "grpc rate limit exceeded")
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
