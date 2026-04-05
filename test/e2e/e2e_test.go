package e2e_test

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"godfs/pkg/client"
	"godfs/test/e2e"
)

func TestE2E_SingleReplicaWriteReadDelete(t *testing.T) {
	const chunkSize = 256 * 1024
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

	if err := cli.Mkdir(ctx, "/data"); err != nil {
		t.Fatal(err)
	}
	if err := cli.Create(ctx, "/data/a.txt"); err != nil {
		t.Fatal(err)
	}
	payload := []byte("hello e2e single replica")
	if err := cli.Write(ctx, "/data/a.txt", payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	out, err := cli.Read(ctx, "/data/a.txt")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(out) != string(payload) {
		t.Fatalf("read mismatch: got %q want %q", out, payload)
	}
	if err := cli.Delete(ctx, "/data/a.txt"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	_, err = cli.Stat(ctx, "/data/a.txt")
	if err == nil {
		t.Fatal("expected stat error after delete")
	}
}

func TestE2E_MultiChunkWrite(t *testing.T) {
	const chunkSize = 4096
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

	if err := cli.Mkdir(ctx, "/w"); err != nil {
		t.Fatal(err)
	}
	if err := cli.Create(ctx, "/w/big.bin"); err != nil {
		t.Fatal(err)
	}
	// Two chunks: 4096 + 4096 = 8192 bytes
	payload := make([]byte, 8192)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	if err := cli.Write(ctx, "/w/big.bin", payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	out, err := cli.Read(ctx, "/w/big.bin")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(out) != len(payload) {
		t.Fatalf("len: got %d want %d", len(out), len(payload))
	}
	for i := range payload {
		if out[i] != payload[i] {
			t.Fatalf("byte %d: got %d want %d", i, out[i], payload[i])
		}
	}
}

func TestE2E_ThreeReplicasSameChunkFile(t *testing.T) {
	const chunkSize = 64 * 1024
	_, cl := e2e.StartMaster(t, chunkSize, 3)
	base := t.TempDir()
	for i := range 3 {
		d := filepath.Join(base, "c"+strconv.Itoa(i))
		if err := os.MkdirAll(d, 0o750); err != nil {
			t.Fatal(err)
		}
		cl.AddChunkServer(t, "node-"+strconv.Itoa(i), d)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cli, err := client.NewWithChunkSize(cl.MasterAddr, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	if err := cli.Mkdir(ctx, "/r"); err != nil {
		t.Fatal(err)
	}
	if err := cli.Create(ctx, "/r/x"); err != nil {
		t.Fatal(err)
	}
	msg := []byte("replicated three ways")
	if err := cli.Write(ctx, "/r/x", msg); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := cli.Read(ctx, "/r/x")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != string(msg) {
		t.Fatalf("read: %q vs %q", got, msg)
	}

	var chkCount int
	for _, d := range cl.ChunkDirs {
		entries, err := os.ReadDir(d)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".chk") {
				chkCount++
			}
		}
	}
	if chkCount != 3 {
		t.Fatalf("expected 3 .chk files across replicas, got %d", chkCount)
	}
}

func TestE2E_RenameAndList(t *testing.T) {
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

	if err := cli.Mkdir(ctx, "/d"); err != nil {
		t.Fatal(err)
	}
	if err := cli.Create(ctx, "/d/old.txt"); err != nil {
		t.Fatal(err)
	}
	if err := cli.Write(ctx, "/d/old.txt", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := cli.Rename(ctx, "/d/old.txt", "/d/new.txt"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	entries, err := cli.List(ctx, "/d")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range entries {
		if e.Name == "new.txt" && !e.IsDir {
			found = true
		}
	}
	if !found {
		t.Fatalf("list after rename: %+v", entries)
	}
}
