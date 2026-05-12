package security

import (
	"testing"
)

func TestHTTPServerTLSConfig_Disabled(t *testing.T) {
	cfg, err := HTTPServerTLSConfig(TLSConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg != nil {
		t.Fatalf("expected nil tls.Config when TLS disabled, got %+v", cfg)
	}
}

func TestHTTPServerTLSConfig_EnabledMissingPaths(t *testing.T) {
	_, err := HTTPServerTLSConfig(TLSConfig{Enabled: true})
	if err == nil {
		t.Fatal("expected error when cert paths missing")
	}
}

func TestLoadRESTListenTLSFromEnv(t *testing.T) {
	t.Setenv("GODFS_REST_HTTPS_ENABLED", "")
	t.Setenv("GODFS_REST_TLS_CERT_FILE", "")
	cfg := LoadRESTListenTLSFromEnv()
	if cfg.Enabled {
		t.Fatalf("expected disabled, got %+v", cfg)
	}

	t.Setenv("GODFS_REST_HTTPS_ENABLED", "1")
	t.Setenv("GODFS_REST_TLS_CERT_FILE", "/tmp/rest.crt")
	t.Setenv("GODFS_REST_TLS_KEY_FILE", "/tmp/rest.key")
	cfg = LoadRESTListenTLSFromEnv()
	if !cfg.Enabled || cfg.CertFile != "/tmp/rest.crt" || cfg.KeyFile != "/tmp/rest.key" {
		t.Fatalf("unexpected cfg: %+v", cfg)
	}
}
