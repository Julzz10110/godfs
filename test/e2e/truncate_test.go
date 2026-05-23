package e2e_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"godfs/pkg/client"
	"godfs/test/e2e"
)

func TestE2E_TruncateFile_shrinkAndSparseExtend(t *testing.T) {
	const chunkSize = 64
	_, cl := e2e.StartMaster(t, chunkSize, 1)
	dir := t.TempDir()
	cl.AddChunkServer(t, "c0", filepath.Join(dir, "c0"))

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cli, err := client.NewWithChunkSize(cl.MasterAddr, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	if err := cli.Mkdir(ctx, "/tr"); err != nil {
		t.Fatal(err)
	}
	if err := cli.Create(ctx, "/tr/f.txt"); err != nil {
		t.Fatal(err)
	}
	payload := []byte("hello-truncate-e2e")
	if err := cli.Write(ctx, "/tr/f.txt", payload); err != nil {
		t.Fatal(err)
	}

	if err := cli.Truncate(ctx, "/tr/f.txt", 2); err != nil {
		t.Fatalf("truncate shrink: %v", err)
	}
	got, err := cli.Read(ctx, "/tr/f.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "he" {
		t.Fatalf("after shrink: %q", got)
	}

	// Sparse extend on a new file (no prior bytes in extended range).
	if err := cli.Create(ctx, "/tr/hole.bin"); err != nil {
		t.Fatal(err)
	}
	if err := cli.Truncate(ctx, "/tr/hole.bin", 8); err != nil {
		t.Fatalf("truncate extend: %v", err)
	}
	st, err := cli.Stat(ctx, "/tr/hole.bin")
	if err != nil {
		t.Fatal(err)
	}
	if st.Size != 8 {
		t.Fatalf("size=%d want 8", st.Size)
	}
	hole, err := cli.Read(ctx, "/tr/hole.bin")
	if err != nil {
		t.Fatal(err)
	}
	if len(hole) != 8 {
		t.Fatalf("len=%d want 8", len(hole))
	}
	for i, b := range hole {
		if b != 0 {
			t.Fatalf("byte %d=%d want zero", i, b)
		}
	}
}
