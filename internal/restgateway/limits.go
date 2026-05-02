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

const defaultRESTReadWriteTimeout = 15 * time.Minute

// slaDurationEnv returns def when key is unset; 0 when set to empty, "0", "off", or "false"; otherwise parsed duration or def on parse error.
func slaDurationEnv(key string, def time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def
	}
	v = strings.TrimSpace(v)
	if v == "" || v == "0" || strings.EqualFold(v, "off") || strings.EqualFold(v, "false") {
		return 0
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		return def
	}
	return d
}

// HTTPServerTimeoutsFromEnv fills ReadHeaderTimeout, ReadTimeout, WriteTimeout, IdleTimeout.
// ReadHeaderTimeout defaults to 10s.
// ReadTimeout / WriteTimeout default to 15m (SLA-friendly for large PUT/GET); set GODFS_REST_READ_TIMEOUT / GODFS_REST_WRITE_TIMEOUT to "0" or "off" to disable.
// IdleTimeout defaults to 120s.
func HTTPServerTimeoutsFromEnv(srv *http.Server) {
	if srv == nil {
		return
	}
	if v := durationEnv("GODFS_REST_READ_HEADER_TIMEOUT", 10*time.Second); v > 0 {
		srv.ReadHeaderTimeout = v
	}
	srv.ReadTimeout = slaDurationEnv("GODFS_REST_READ_TIMEOUT", defaultRESTReadWriteTimeout)
	srv.WriteTimeout = slaDurationEnv("GODFS_REST_WRITE_TIMEOUT", defaultRESTReadWriteTimeout)
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
