package restgateway

import (
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type corsConfig struct {
	enabled      bool
	allowOrigins map[string]struct{}
	allowAll     bool
	allowMethods string
	allowHeaders string
	exposeHeaders string
	allowCredentials bool
	maxAgeSeconds int
}

func corsFromEnv() corsConfig {
	origins := strings.TrimSpace(getenv("GODFS_REST_CORS_ALLOW_ORIGINS", ""))
	if origins == "" {
		return corsConfig{enabled: false}
	}
	cfg := corsConfig{
		enabled:      true,
		allowOrigins: map[string]struct{}{},
		allowMethods: getenv("GODFS_REST_CORS_ALLOW_METHODS", "GET,HEAD,POST,PUT,DELETE,OPTIONS"),
		allowHeaders: getenv("GODFS_REST_CORS_ALLOW_HEADERS", "Authorization,Content-Type,Range,If-Range,If-None-Match,If-Modified-Since"),
		exposeHeaders: getenv("GODFS_REST_CORS_EXPOSE_HEADERS", "ETag,Last-Modified,Content-Range,Accept-Ranges,Content-Length"),
		allowCredentials: getenv("GODFS_REST_CORS_ALLOW_CREDENTIALS", "") == "1" || strings.EqualFold(getenv("GODFS_REST_CORS_ALLOW_CREDENTIALS", ""), "true"),
		maxAgeSeconds: atoi(getenv("GODFS_REST_CORS_MAX_AGE", "600"), 600),
	}
	for _, o := range strings.Split(origins, ",") {
		o = strings.TrimSpace(o)
		if o == "" {
			continue
		}
		if o == "*" {
			cfg.allowAll = true
			continue
		}
		cfg.allowOrigins[o] = struct{}{}
	}
	return cfg
}

func WithCORS(next http.Handler) http.Handler {
	cfg := corsFromEnv()
	if !cfg.enabled {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := strings.TrimSpace(r.Header.Get("Origin"))
		if origin != "" {
			allowed := cfg.allowAll
			if !allowed {
				_, allowed = cfg.allowOrigins[origin]
			}
			if allowed {
				w.Header().Set("Vary", "Origin")
				w.Header().Set("Access-Control-Allow-Origin", origin)
				if cfg.allowCredentials {
					w.Header().Set("Access-Control-Allow-Credentials", "true")
				}
				if cfg.exposeHeaders != "" {
					w.Header().Set("Access-Control-Expose-Headers", cfg.exposeHeaders)
				}
			}
		}

		// Preflight
		if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
			w.Header().Set("Access-Control-Allow-Methods", cfg.allowMethods)
			w.Header().Set("Access-Control-Allow-Headers", cfg.allowHeaders)
			if cfg.maxAgeSeconds > 0 {
				w.Header().Set("Access-Control-Max-Age", strconv.Itoa(cfg.maxAgeSeconds))
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

type limiterEntry struct {
	lim  *rate.Limiter
	last time.Time
}

type rateLimiter struct {
	mu      sync.Mutex
	entries map[string]*limiterEntry
	r       rate.Limit
	burst   int
	ttl     time.Duration
}

func (rl *rateLimiter) get(key string) *rate.Limiter {
	now := time.Now()
	rl.mu.Lock()
	defer rl.mu.Unlock()
	if rl.entries == nil {
		rl.entries = make(map[string]*limiterEntry)
	}
	if e := rl.entries[key]; e != nil {
		e.last = now
		return e.lim
	}
	lim := rate.NewLimiter(rl.r, rl.burst)
	rl.entries[key] = &limiterEntry{lim: lim, last: now}
	return lim
}

func (rl *rateLimiter) cleanup() {
	if rl.ttl <= 0 {
		rl.ttl = 10 * time.Minute
	}
	cut := time.Now().Add(-rl.ttl)
	rl.mu.Lock()
	defer rl.mu.Unlock()
	for k, e := range rl.entries {
		if e == nil || e.last.Before(cut) {
			delete(rl.entries, k)
		}
	}
}

func WithRateLimit(next http.Handler) http.Handler {
	rpsStr := strings.TrimSpace(getenv("GODFS_REST_RATE_LIMIT_RPS", ""))
	if rpsStr == "" {
		return next
	}
	rps, err := strconv.ParseFloat(rpsStr, 64)
	if err != nil || rps <= 0 {
		return next
	}
	burst := atoi(getenv("GODFS_REST_RATE_LIMIT_BURST", "10"), 10)
	ttlSec := atoi(getenv("GODFS_REST_RATE_LIMIT_TTL_SECONDS", "600"), 600)
	rl := &rateLimiter{
		r:     rate.Limit(rps),
		burst: burst,
		ttl:   time.Duration(ttlSec) * time.Second,
	}
	// best-effort cleanup
	ticker := time.NewTicker(rl.ttl / 2)
	if rl.ttl < 30*time.Second {
		ticker = time.NewTicker(30 * time.Second)
	}
	go func() {
		for range ticker.C {
			rl.cleanup()
		}
	}()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := rateLimitKey(r)
		if !rl.get(key).Allow() {
			writeJSON(w, http.StatusTooManyRequests, errJSON{
				Error:      "rate limit exceeded",
				Code:       "rate_limited",
				HTTPStatus: http.StatusTooManyRequests,
			})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func rateLimitKey(r *http.Request) string {
	// Prefer bearer token as principal key (per-user), fallback to client IP.
	if auth := strings.TrimSpace(r.Header.Get("Authorization")); auth != "" {
		return "auth:" + auth
	}
	host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err == nil && host != "" {
		return "ip:" + host
	}
	if r.RemoteAddr != "" {
		return "ip:" + r.RemoteAddr
	}
	return "ip:unknown"
}

func getenv(k, def string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return def
}

func atoi(s string, def int) int {
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil || n < 0 {
		return def
	}
	return n
}

