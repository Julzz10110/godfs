package e2e_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"godfs/pkg/client"
	"godfs/test/e2e"
)

// BenchmarkE2E_SingleChunkWrite_1Replica measures end-to-end Write (PrepareWrite → chunk → Commit) throughput.
func BenchmarkE2E_SingleChunkWrite_1Replica(b *testing.B) {
	if testing.Short() {
		b.Skip()
	}
	const chunkSize = 256 * 1024
	_, cl := e2e.StartMaster(b, chunkSize, 1)
	dir := b.TempDir()
	cl.AddChunkServer(b, "chunk-a", filepath.Join(dir, "c0"))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	cli, err := client.NewWithChunkSize(cl.MasterAddr, chunkSize)
	if err != nil {
		b.Fatal(err)
	}
	defer cli.Close()

	if err := cli.Mkdir(ctx, "/bench"); err != nil {
		b.Fatal(err)
	}
	if err := cli.Create(ctx, "/bench/w"); err != nil {
		b.Fatal(err)
	}

	payload := make([]byte, chunkSize)
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := cli.Write(ctx, "/bench/w", payload); err != nil {
			b.Fatal(err)
		}
	}
}
