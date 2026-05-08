package security

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTLSReloadInterval_DefaultAndClamp(t *testing.T) {
	t.Setenv("GODFS_TLS_RELOAD_INTERVAL", "")
	if got := tlsReloadInterval(); got <= 0 {
		t.Fatalf("expected default interval, got %s", got)
	}
	t.Setenv("GODFS_TLS_RELOAD_INTERVAL", "1ms")
	if got := tlsReloadInterval(); got < 500_000_000 { // 500ms
		t.Fatalf("expected clamp to >=500ms, got %s", got)
	}
}

func TestCertReloader_ReloadsCA(t *testing.T) {
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(caPath, []byte("NOT A CERT"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := newCertReloader(TLSConfig{CAFile: caPath})
	if err == nil {
		t.Fatal("expected error for invalid CA PEM")
	}
}

