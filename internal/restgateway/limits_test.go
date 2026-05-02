package restgateway

import (
	"net/http"
	"testing"
	"time"
)

func TestHTTPServerTimeoutsFromEnv_defaults(t *testing.T) {
	t.Setenv("GODFS_REST_READ_HEADER_TIMEOUT", "")
	t.Setenv("GODFS_REST_READ_TIMEOUT", "")
	t.Setenv("GODFS_REST_WRITE_TIMEOUT", "")
	t.Setenv("GODFS_REST_IDLE_TIMEOUT", "")

	s := &http.Server{}
	HTTPServerTimeoutsFromEnv(s)
	if s.ReadHeaderTimeout != 10*time.Second {
		t.Fatalf("ReadHeaderTimeout=%v", s.ReadHeaderTimeout)
	}
	if s.ReadTimeout != 0 || s.WriteTimeout != 0 {
		t.Fatalf("ReadTimeout=%v WriteTimeout=%v", s.ReadTimeout, s.WriteTimeout)
	}
	if s.IdleTimeout != 120*time.Second {
		t.Fatalf("IdleTimeout=%v", s.IdleTimeout)
	}
}

func TestDefaultGetStreamBytes(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		t.Setenv("GODFS_REST_GET_STREAM_BYTES", "")
		if n := DefaultGetStreamBytes(); n != 4<<20 {
			t.Fatalf("got %d", n)
		}
	})
	t.Run("custom", func(t *testing.T) {
		t.Setenv("GODFS_REST_GET_STREAM_BYTES", "65536")
		if n := DefaultGetStreamBytes(); n != 65536 {
			t.Fatalf("got %d", n)
		}
	})
	t.Run("belowMinFallsBack", func(t *testing.T) {
		t.Setenv("GODFS_REST_GET_STREAM_BYTES", "100")
		if n := DefaultGetStreamBytes(); n != 4<<20 {
			t.Fatalf("got %d want default", n)
		}
	})
}
