//go:build linux

package main

import (
	"context"
	"testing"

	godfsv1 "godfs/api/proto/godfs/v1"
	"godfs/pkg/client"
)

type stubFUSECLI struct {
	truncPath  string
	truncSize  int64
	writeCalls []struct {
		off  int64
		data []byte
	}
}

func (s *stubFUSECLI) Stat(context.Context, string) (*client.FileInfo, error) {
	return &client.FileInfo{Size: 0}, nil
}

func (s *stubFUSECLI) List(context.Context, string) ([]*godfsv1.DirEntry, error) {
	return nil, nil
}

func (s *stubFUSECLI) Create(context.Context, string) error { return nil }

func (s *stubFUSECLI) Mkdir(context.Context, string) error { return nil }

func (s *stubFUSECLI) Delete(context.Context, string) error { return nil }

func (s *stubFUSECLI) Rename(context.Context, string, string) error { return nil }

func (s *stubFUSECLI) ReadRange(context.Context, string, int64, int64) ([]byte, error) {
	return make([]byte, 0), nil
}

func (s *stubFUSECLI) WriteAt(_ context.Context, _ string, off int64, data []byte) error {
	s.writeCalls = append(s.writeCalls, struct {
		off  int64
		data []byte
	}{off, append([]byte(nil), data...)})
	return nil
}

func (s *stubFUSECLI) Truncate(_ context.Context, path string, size int64) error {
	s.truncPath = path
	s.truncSize = size
	return nil
}

func TestNode_truncateTo_callsTruncate(t *testing.T) {
	stub := &stubFUSECLI{}
	n := &node{cli: stub, full: "/f.txt", size: 100}
	if errno := n.truncateTo(context.Background(), nil, 0); errno != 0 {
		t.Fatalf("errno %v", errno)
	}
	if stub.truncPath != "/f.txt" || stub.truncSize != 0 {
		t.Fatalf("truncate path=%q size=%d", stub.truncPath, stub.truncSize)
	}
	if n.size != 0 {
		t.Fatalf("node size=%d", n.size)
	}
}

func TestFileHandle_bufferedWriteFlushesOnFlush(t *testing.T) {
	stub := &stubFUSECLI{}
	n := &node{cli: stub, full: "/w.txt"}
	fh := &fileHandle{n: n, writeable: true, buf: &fuseWriteBuffer{}}
	if _, errno := fh.Write(context.Background(), []byte("ab"), 0); errno != 0 {
		t.Fatalf("write errno %v", errno)
	}
	if len(stub.writeCalls) != 0 {
		t.Fatal("expected no RPC before flush")
	}
	if errno := fh.Flush(context.Background()); errno != 0 {
		t.Fatalf("flush errno %v", errno)
	}
	if len(stub.writeCalls) != 1 || string(stub.writeCalls[0].data) != "ab" {
		t.Fatalf("writes=%+v", stub.writeCalls)
	}
}

func TestFileHandle_bufferedReadSeesUnflushedData(t *testing.T) {
	stub := &stubFUSECLI{}
	n := &node{cli: stub, full: "/r.txt"}
	fh := &fileHandle{n: n, writeable: true, buf: &fuseWriteBuffer{}}
	_, _ = fh.Write(context.Background(), []byte("xy"), 1)
	dest := make([]byte, 2)
	overlayBuffered(dest, 1, fh.buf)
	if string(dest) != "xy" {
		t.Fatalf("overlay got %q", dest)
	}
}
