package e2e_test

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"godfs/pkg/client"
	"godfs/test/e2e"
)

// TestE2E_LargeFile exercises multi-chunk write/read; size from GODFS_E2E_LARGE_BYTES (default 8 MiB).
// Set to ~1GiB for a full stress run: GODFS_E2E_LARGE_BYTES=1073741824
func TestE2E_LargeFile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large file e2e in -short mode")
	}
	const defaultBytes = 8 * 1024 * 1024
	n := defaultBytes
	if v := os.Getenv("GODFS_E2E_LARGE_BYTES"); v != "" {
		var err error
		n, err = strconv.Atoi(v)
		if err != nil || n <= 0 {
			t.Fatalf("invalid GODFS_E2E_LARGE_BYTES: %q", v)
		}
	}
	const chunkSize = 256 * 1024
	_, cl := e2e.StartMaster(t, chunkSize, 1)
	dir := t.TempDir()
	cl.AddChunkServer(t, "chunk-a", filepath.Join(dir, "c0"))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	cli, err := client.NewWithChunkSize(cl.MasterAddr, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	if err := cli.Mkdir(ctx, "/big"); err != nil {
		t.Fatal(err)
	}
	if err := cli.Create(ctx, "/big/file.bin"); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, n)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	if err := cli.Write(ctx, "/big/file.bin", payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	out, err := cli.Read(ctx, "/big/file.bin")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(out) != len(payload) {
		t.Fatalf("len %d want %d", len(out), len(payload))
	}
	for i := range payload {
		if out[i] != payload[i] {
			t.Fatalf("byte mismatch at %d", i)
		}
	}
}
