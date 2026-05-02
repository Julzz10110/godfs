package restgateway

import (
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// DefaultGetStreamBytes is the max bytes read per internal ReadRange segment when streaming GET responses.
// GODFS_REST_GET_STREAM_BYTES (default 4 MiB, minimum 64 KiB).
func DefaultGetStreamBytes() int64 {
	const def = 4 << 20
	const min = 64 << 10
	s := strings.TrimSpace(os.Getenv("GODFS_REST_GET_STREAM_BYTES"))
	if s == "" {
		return def
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < min {
		return def
	}
	return n
}

// DefaultMaxJSONBodyBytes limits JSON request bodies (mkdir, rename, snapshots, etc.).
// GODFS_REST_MAX_JSON_BODY_BYTES (default 1 MiB).
func DefaultMaxJSONBodyBytes() int64 {
	const def = 1 << 20
	s := strings.TrimSpace(os.Getenv("GODFS_REST_MAX_JSON_BODY_BYTES"))
	if s == "" {
		return def
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 {
		return def
	}
	return n
}

// HTTPServerTimeoutsFromEnv fills ReadHeaderTimeout, ReadTimeout, WriteTimeout, IdleTimeout.
// ReadHeaderTimeout defaults to 10s. ReadTimeout/WriteTimeout default to 0 (disabled) for large uploads/downloads.
// IdleTimeout defaults to 120s.
func HTTPServerTimeoutsFromEnv(srv *http.Server) {
	if srv == nil {
		return
	}
	if v := durationEnv("GODFS_REST_READ_HEADER_TIMEOUT", 10*time.Second); v > 0 {
		srv.ReadHeaderTimeout = v
	}
	srv.ReadTimeout = durationEnvOrZero("GODFS_REST_READ_TIMEOUT")
	srv.WriteTimeout = durationEnvOrZero("GODFS_REST_WRITE_TIMEOUT")
	if v := durationEnv("GODFS_REST_IDLE_TIMEOUT", 120*time.Second); v > 0 {
		srv.IdleTimeout = v
	}
}

func durationEnv(key string, def time.Duration) time.Duration {
	s := strings.TrimSpace(os.Getenv(key))
	if s == "" {
		return def
	}
	d, err := time.ParseDuration(s)
	if err != nil || d < 0 {
		return def
	}
	return d
}

func durationEnvOrZero(key string) time.Duration {
	s := strings.TrimSpace(os.Getenv(key))
	if s == "" {
		return 0
	}
	d, err := time.ParseDuration(s)
	if err != nil || d < 0 {
		return 0
	}
	return d
}
