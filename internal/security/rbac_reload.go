package security

import (
	"context"
	"log"
	"os"
	"strings"
	"time"
)

// RBACReloadInterval returns reload cadence when GODFS_RBAC_JSON is a @file path.
// Zero disables periodic reload.
func RBACReloadInterval() time.Duration {
	s := strings.TrimSpace(os.Getenv("GODFS_RBAC_RELOAD_INTERVAL"))
	if s == "" || s == "0" {
		return 0
	}
	d, err := time.ParseDuration(s)
	if err != nil || d < 2*time.Second {
		return 0
	}
	return d
}

// LoopRBACFileReload periodically re-reads the RBAC JSON file from GODFS_RBAC_JSON (@path) and swaps rules.
func LoopRBACFileReload(ctx context.Context, holder *RBACHolder, every time.Duration) {
	if holder == nil || every <= 0 {
		return
	}
	rawEnv := strings.TrimSpace(os.Getenv("GODFS_RBAC_JSON"))
	if !strings.HasPrefix(rawEnv, "@") {
		return
	}
	path := strings.TrimPrefix(rawEnv, "@")
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			b, err := os.ReadFile(path)
			if err != nil {
				log.Printf("rbac reload: read %s: %v", path, err)
				continue
			}
			content := strings.TrimSpace(string(b))
			next, err := NewRBAC(content, content == "")
			if err != nil {
				log.Printf("rbac reload: parse: %v", err)
				continue
			}
			holder.Store(next)
		}
	}
}
