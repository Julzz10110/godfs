package security

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAuthReload_SwapsAPIKeys(t *testing.T) {
	dir := t.TempDir()
	keysPath := filepath.Join(dir, "keys")
	if err := os.WriteFile(keysPath, []byte("alice:oldkey"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GODFS_API_KEYS", "@"+keysPath)
	t.Setenv("GODFS_CLUSTER_KEY", "")
	t.Setenv("GODFS_JWT_HMAC_SECRET", "")
	t.Setenv("GODFS_JWT_JWKS_URL", "")

	a0, err := LoadAuthFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	h := NewAuthHolder(a0)
	if a0.APIKeyToUser["oldkey"] != "alice" {
		t.Fatalf("keys=%v", a0.APIKeyToUser)
	}

	if err := os.WriteFile(keysPath, []byte("bob:newkey"), 0o600); err != nil {
		t.Fatal(err)
	}
	next, err := LoadAuthFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	h.Store(next)
	cur := h.Current()
	if cur.APIKeyToUser["newkey"] != "bob" {
		t.Fatalf("after reload keys=%v", cur.APIKeyToUser)
	}
	if _, ok := cur.APIKeyToUser["oldkey"]; ok {
		t.Fatal("old key should be gone")
	}
}

func TestAuthReloadInterval_Clamp(t *testing.T) {
	t.Setenv("GODFS_AUTH_RELOAD_INTERVAL", "1ms")
	if got := AuthReloadInterval(); got != 0 {
		t.Fatalf("expected 0 for too-short interval, got %v", got)
	}
	t.Setenv("GODFS_AUTH_RELOAD_INTERVAL", "5s")
	if got := AuthReloadInterval(); got != 5*time.Second {
		t.Fatalf("got %v", got)
	}
}

func TestLoadAuthFromEnv_ClusterKeyFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "cluster")
	if err := os.WriteFile(p, []byte("  cluster-secret  \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GODFS_CLUSTER_KEY", "@"+p)
	t.Setenv("GODFS_API_KEYS", "")
	t.Setenv("GODFS_JWT_HMAC_SECRET", "")
	t.Setenv("GODFS_JWT_JWKS_URL", "")

	a, err := LoadAuthFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if a.ClusterKey != "cluster-secret" {
		t.Fatalf("cluster key=%q", a.ClusterKey)
	}
}
