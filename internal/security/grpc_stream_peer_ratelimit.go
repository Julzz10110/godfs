package security

import (
	"os"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// parseGRPCPeerStreamLimitEnv reads GODFS_GRPC_PEER_STREAM_MAX_CONCURRENT (0 = disabled).
func parseGRPCPeerStreamLimitEnv() int {
	v := strings.TrimSpace(os.Getenv("GODFS_GRPC_PEER_STREAM_MAX_CONCURRENT"))
	if v == "" {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0
	}
	return n
}

type peerStreamLimiter struct {
	max    int
	mu     sync.Mutex
	active map[string]int
}

func newPeerStreamLimiterFromEnv() *peerStreamLimiter {
	max := parseGRPCPeerStreamLimitEnv()
	if max <= 0 {
		return nil
	}
	return &peerStreamLimiter{
		max:    max,
		active: make(map[string]int),
	}
}

func (pl *peerStreamLimiter) acquire(key string) error {
	if key == "" {
		key = "anon"
	}
	pl.mu.Lock()
	defer pl.mu.Unlock()
	n := pl.active[key]
	if n >= pl.max {
		return status.Errorf(codes.ResourceExhausted, "grpc peer stream limit exceeded (max %d concurrent streams per peer)", pl.max)
	}
	pl.active[key] = n + 1
	return nil
}

func (pl *peerStreamLimiter) release(key string) {
	if key == "" {
		key = "anon"
	}
	pl.mu.Lock()
	defer pl.mu.Unlock()
	n := pl.active[key]
	if n <= 1 {
		delete(pl.active, key)
		return
	}
	pl.active[key] = n - 1
}

func subjectToGRPCPeerStreamLimit(fullMethod string) bool {
	switch fullMethod {
	case "/godfs.v1.ChunkService/ReadChunk", "/godfs.v1.ChunkService/PullChunk":
		return true
	default:
		return false
	}
}

// GRPCStreamPeerRateLimitFromEnv limits concurrent ChunkService streaming RPCs per peer identity.
//
// Env: GODFS_GRPC_PEER_STREAM_MAX_CONCURRENT — max simultaneous streams per caller
// (mTLS client CN, else Bearer token hash). Master RegisterNode/Heartbeat are unary-only and unaffected.
func GRPCStreamPeerRateLimitFromEnv() grpc.StreamServerInterceptor {
	lim := newPeerStreamLimiterFromEnv()
	if lim == nil {
		return nil
	}
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if !subjectToGRPCPeerStreamLimit(info.FullMethod) {
			return handler(srv, ss)
		}
		key := grpcRateLimitPeerKey(ss.Context())
		if err := lim.acquire(key); err != nil {
			return err
		}
		defer lim.release(key)
		return handler(srv, ss)
	}
}
