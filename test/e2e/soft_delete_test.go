package e2e_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"godfs/pkg/client"
	"godfs/test/e2e"
)

func TestE2E_SoftDelete_RestoreWithinGrace(t *testing.T) {
	const chunkSize = 32 * 1024
	store, cl := e2e.StartMaster(t, chunkSize, 1)
	store.SetSoftDeleteGrace(time.Hour)
	dir := t.TempDir()
	cl.AddChunkServer(t, "chunk-a", filepath.Join(dir, "c0"))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cli, err := client.NewWithChunkSize(cl.MasterAddr, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	_ = cli.Mkdir(ctx, "/sd")
	_ = cli.Create(ctx, "/sd/f")
	_ = cli.Write(ctx, "/sd/f", []byte("soft-delete"))

	if err := cli.Delete(ctx, "/sd/f"); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Stat(ctx, "/sd/f"); err == nil {
		t.Fatal("file should be hidden in trash")
	}
	if err := cli.RestoreFile(ctx, "/sd/f"); err != nil {
		t.Fatal(err)
	}
	st, err := cli.Stat(ctx, "/sd/f")
	if err != nil {
		t.Fatal(err)
	}
	if st.Size != 11 {
		t.Fatalf("size=%d", st.Size)
	}
}
