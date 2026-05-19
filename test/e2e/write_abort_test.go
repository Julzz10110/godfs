package e2e_test

import (
	"context"
	"io"
	"path/filepath"
	"testing"

	"godfs/pkg/client"
	"godfs/test/e2e"
)

type abortReader struct {
	b      []byte
	off    int
	failAt int
}

func (r *abortReader) Read(p []byte) (int, error) {
	if r.off >= r.failAt {
		return 0, io.ErrUnexpectedEOF
	}
	n := copy(p, r.b[r.off:])
	if r.off+n > r.failAt {
		n = r.failAt - r.off
	}
	r.off += n
	if r.off >= r.failAt {
		return n, io.ErrUnexpectedEOF
	}
	return n, nil
}

// WriteFromReader error mid-stream must not expose a fully written object.
func TestE2E_WriteFromReaderAbort(t *testing.T) {
	const chunkSize = 64 * 1024
	_, cl := e2e.StartMaster(t, chunkSize, 1)
	dir := t.TempDir()
	cl.AddChunkServer(t, "chunk-a", filepath.Join(dir, "c0"))

	ctx := context.Background()
	cli, err := client.NewWithChunkSize(cl.MasterAddr, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	path := "/abort.bin"
	if err := cli.Create(ctx, path); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, chunkSize)
	if err := cli.WriteFromReader(ctx, path, &abortReader{b: payload, failAt: 4096}); err == nil {
		t.Fatal("expected WriteFromReader error")
	}
	st, err := cli.Stat(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size >= int64(len(payload)) {
		t.Fatalf("file size %d >= full payload after early abort", st.Size)
	}
}
