package e2e_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"godfs/pkg/client"
	"godfs/test/e2e"
)

func TestE2E_BackupSnapshot(t *testing.T) {
	const chunkSize = 64 * 1024
	_, cl := e2e.StartMaster(t, chunkSize, 1)
	dir := t.TempDir()
	cl.AddChunkServer(t, "chunk-a", filepath.Join(dir, "c0"))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cli, err := client.NewWithChunkSize(cl.MasterAddr, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	if err := cli.Mkdir(ctx, "/snap"); err != nil {
		t.Fatal(err)
	}
	if err := cli.Create(ctx, "/snap/a.txt"); err != nil {
		t.Fatal(err)
	}
	payload := []byte("snapshot manifest e2e")
	if err := cli.Write(ctx, "/snap/a.txt", payload); err != nil {
		t.Fatalf("write: %v", err)
	}

	id, _, err := cli.CreateSnapshot(ctx, "e2e-label")
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if id == "" {
		t.Fatal("empty snapshot id")
	}

	list, err := cli.ListSnapshots(ctx)
	if err != nil {
		t.Fatalf("ListSnapshots: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("snapshots: got %d want 1", len(list))
	}
	if list[0].GetSnapshotId() != id {
		t.Fatalf("id mismatch")
	}
	if list[0].GetLabel() != "e2e-label" {
		t.Fatalf("label: got %q", list[0].GetLabel())
	}
	if list[0].GetFileCount() < 1 {
		t.Fatalf("file_count: got %d", list[0].GetFileCount())
	}

	man, err := cli.GetSnapshot(ctx, id)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if man.GetSnapshotId() != id {
		t.Fatal("manifest id mismatch")
	}
	var found bool
	for _, f := range man.GetFiles() {
		if f.GetPath() == "/snap/a.txt" {
			found = true
			if len(f.GetChunks()) == 0 {
				t.Fatal("expected chunks for /snap/a.txt")
			}
			break
		}
	}
	if !found {
		t.Fatal("manifest missing /snap/a.txt")
	}

	if err := cli.DeleteSnapshot(ctx, id); err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}
	list2, err := cli.ListSnapshots(ctx)
	if err != nil {
		t.Fatalf("ListSnapshots after delete: %v", err)
	}
	if len(list2) != 0 {
		t.Fatalf("expected 0 snapshots after delete, got %d", len(list2))
	}
}
