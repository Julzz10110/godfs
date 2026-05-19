package containers_test

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/compose"
)

// programmatic compose up using testcontainers-go (isolated module).
func TestComposeStack_Health(t *testing.T) {
	if os.Getenv("GODFS_TESTCONTAINERS") == "" {
		t.Skip("set GODFS_TESTCONTAINERS=1 to run (non-blocking CI job)")
	}
	if runtime.GOOS == "windows" {
		t.Skip("docker compose test requires Linux/macOS CI")
	}

	root, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)
	composeFile := filepath.Join(root, "deployments", "docker", "docker-compose.yml")

	stack, err := compose.NewDockerCompose(composeFile)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = stack.Down(context.Background(), compose.RemoveOrphans(true), compose.RemoveVolumes(true))
	})

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	require.NoError(t, stack.Up(ctx, compose.Wait(true)))

	var lastErr error
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://127.0.0.1:8080/v1/health")
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
			lastErr = err
			t.Logf("health status=%d body=%s", resp.StatusCode, body)
		} else {
			lastErr = err
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("REST /v1/health not ready: %v", lastErr)
}
