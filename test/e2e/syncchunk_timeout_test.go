package e2e_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"godfs/pkg/client"
	"godfs/test/e2e"
)

// stalled SyncChunk on a secondary must fail the write; metadata must not show a full commit.
func TestE2E_SyncChunkStallOnReplicate(t *testing.T) {
	const chunkSize = 64 * 1024
	_, cl := e2e.StartMaster(t, chunkSize, 2)
	dir := t.TempDir()
	cl.AddChunkServer(t, "chunk-a", filepath.Join(dir, "a"))
	cl.AddStallSyncChunkServer(t, "chunk-b", filepath.Join(dir, "b"))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli, err := client.NewWithChunkSize(cl.MasterAddr, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	path := "/syncstall.bin"
	if err := cli.Create(ctx, path); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 8*1024)
	writeErr := cli.Write(ctx, path, payload)
	if writeErr == nil {
		t.Fatal("expected write error when SyncChunk stalls on secondary")
	}
	t.Logf("write failed as expected: %v", writeErr)

	stCtx, stCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer stCancel()
	st, err := cli.Stat(stCtx, path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size >= int64(len(payload)) {
		t.Fatalf("file committed full size %d after failed replicate (orphan/partial policy: size must stay below payload)", st.Size)
	}
}
