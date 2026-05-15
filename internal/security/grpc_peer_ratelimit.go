package security

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

// parseGRPCPeerRateLimitEnv reads GODFS_GRPC_PEER_RATE_LIMIT_RPS and _BURST.
func parseGRPCPeerRateLimitEnv() (rps float64, burst int) {
	v := strings.TrimSpace(os.Getenv("GODFS_GRPC_PEER_RATE_LIMIT_RPS"))
	if v == "" {
		return 0, 0
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f <= 0 {
		return 0, 0
	}
	rps = f
	if s := strings.TrimSpace(os.Getenv("GODFS_GRPC_PEER_RATE_LIMIT_BURST")); s != "" {
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

type peerRateLimiter struct {
	r       rate.Limit
	burst   int
	ttl     time.Duration
	mu      sync.Mutex
	buckets map[string]*rate.Limiter
	seen    map[string]time.Time
}

func newPeerRateLimiterFromEnv() *peerRateLimiter {
	rps, burst := parseGRPCPeerRateLimitEnv()
	if rps <= 0 || burst <= 0 {
		return nil
	}
	ttlSec := 600
	if s := strings.TrimSpace(os.Getenv("GODFS_GRPC_PEER_RATE_LIMIT_TTL_SECONDS")); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			ttlSec = n
		}
	}
	return &peerRateLimiter{
		r:       rate.Limit(rps),
		burst:   burst,
		ttl:     time.Duration(ttlSec) * time.Second,
		buckets: make(map[string]*rate.Limiter),
		seen:    make(map[string]time.Time),
	}
}

func (pl *peerRateLimiter) allow(key string) bool {
	if key == "" {
		key = "anon"
	}
	now := time.Now()
	pl.mu.Lock()
	defer pl.mu.Unlock()
	if t, ok := pl.seen[key]; ok && now.Sub(t) > pl.ttl {
		delete(pl.buckets, key)
		delete(pl.seen, key)
	}
	lim, ok := pl.buckets[key]
	if !ok {
		lim = rate.NewLimiter(pl.r, pl.burst)
		pl.buckets[key] = lim
	}
	pl.seen[key] = now
	if len(pl.seen) > 4096 {
		pl.evictLocked(now)
	}
	return lim.Allow()
}

func (pl *peerRateLimiter) evictLocked(now time.Time) {
	for k, t := range pl.seen {
		if now.Sub(t) > pl.ttl {
			delete(pl.buckets, k)
			delete(pl.seen, k)
		}
	}
}

// grpcRateLimitPeerKey identifies the caller for per-peer limits: mTLS client CN, else Bearer hash, else "anon".
func grpcRateLimitPeerKey(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok && p != nil {
		if ti, ok := p.AuthInfo.(credentials.TLSInfo); ok && len(ti.State.PeerCertificates) > 0 {
			cn := strings.TrimSpace(ti.State.PeerCertificates[0].Subject.CommonName)
			if cn != "" {
				return "tls:" + cn
			}
		}
	}
	if tok := bearerTokenFromContext(ctx); tok != "" {
		sum := sha256.Sum256([]byte(tok))
		return "bearer:" + hex.EncodeToString(sum[:8])
	}
	return "anon"
}

func bearerTokenFromContext(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	return BearerFromMD(md)
}

// BearerFromMD extracts the bearer token from gRPC metadata.
func BearerFromMD(md metadata.MD) string {
	for _, k := range []string{"authorization", "Authorization"} {
		v := md.Get(k)
		if len(v) == 0 {
			continue
		}
		tok := strings.TrimSpace(v[0])
		if len(tok) >= 7 && strings.EqualFold(tok[:7], "bearer ") {
			return strings.TrimSpace(tok[7:])
		}
		return tok
	}
	return ""
}
