package security

import (
	"context"
	"log"
	"os"
	"strings"
	"time"
)

// AuthReloadInterval returns reload cadence when GODFS_API_KEYS or GODFS_CLUSTER_KEY uses a @file path.
// Env: GODFS_AUTH_RELOAD_INTERVAL (minimum 2s). Zero disables periodic reload.
func AuthReloadInterval() time.Duration {
	s := strings.TrimSpace(os.Getenv("GODFS_AUTH_RELOAD_INTERVAL"))
	if s == "" || s == "0" {
		return 0
	}
	d, err := time.ParseDuration(s)
	if err != nil || d < 2*time.Second {
		return 0
	}
	return d
}

func authEnvUsesFile() bool {
	for _, key := range []string{"GODFS_API_KEYS", "GODFS_CLUSTER_KEY"} {
		v := strings.TrimSpace(os.Getenv(key))
		if strings.HasPrefix(v, "@") {
			return true
		}
	}
	return false
}

// LoopAuthFileReload periodically re-reads auth material from env (@file paths) and swaps the holder.
func LoopAuthFileReload(ctx context.Context, holder *AuthHolder, every time.Duration) {
	if holder == nil || every <= 0 || !authEnvUsesFile() {
		return
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			next, err := LoadAuthFromEnv()
			if err != nil {
				log.Printf("auth reload: %v", err)
				continue
			}
			holder.Store(next)
		}
	}
}
