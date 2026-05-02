package restgateway

import (
	"os"
	"testing"
)

func TestDefaultMaxUploadBytes_fallbackWhenUnset(t *testing.T) {
	prev, had := os.LookupEnv("GODFS_REST_MAX_UPLOAD_BYTES")
	if had {
		_ = os.Unsetenv("GODFS_REST_MAX_UPLOAD_BYTES")
	}
	t.Cleanup(func() {
		if had {
			_ = os.Setenv("GODFS_REST_MAX_UPLOAD_BYTES", prev)
		} else {
			_ = os.Unsetenv("GODFS_REST_MAX_UPLOAD_BYTES")
		}
	})
	t.Setenv("GODFS_REST_MAX_BODY_BYTES", "12345")
	if got := DefaultMaxUploadBytes(); got != 12345 {
		t.Fatalf("got %d want 12345", got)
	}
}

func TestDefaultMaxUploadBytes_explicitZeroUnlimited(t *testing.T) {
	t.Setenv("GODFS_REST_MAX_UPLOAD_BYTES", "0")
	if got := DefaultMaxUploadBytes(); got != 0 {
		t.Fatalf("got %d want 0", got)
	}
}

func TestDefaultMaxUploadBytes_positive(t *testing.T) {
	t.Setenv("GODFS_REST_MAX_UPLOAD_BYTES", "999")
	if got := DefaultMaxUploadBytes(); got != 999 {
		t.Fatalf("got %d want 999", got)
	}
}

func TestDefaultMaxUploadBytes_negativeUnlimited(t *testing.T) {
	t.Setenv("GODFS_REST_MAX_UPLOAD_BYTES", "-1")
	if got := DefaultMaxUploadBytes(); got != 0 {
		t.Fatalf("got %d want 0", got)
	}
}

func TestServer_putUploadLimit(t *testing.T) {
	t.Setenv("GODFS_REST_MAX_UPLOAD_BYTES", "5000")
	s := &Server{MaxUpload: 0}
	if got := s.putUploadLimit(); got != 5000 {
		t.Fatalf("got %d want 5000", got)
	}
	s.MaxUpload = 100
	if got := s.putUploadLimit(); got != 100 {
		t.Fatalf("got %d want 100", got)
	}
	s.MaxUpload = NoMaxUploadLimit
	if got := s.putUploadLimit(); got != 0 {
		t.Fatalf("got %d want 0", got)
	}
}
