package main

import (
	"context"
	"sync/atomic"
	"testing"

	"godfs/internal/domain"
)

func TestWrapMaintChecksumVerifier_CallsInner(t *testing.T) {
	var calls atomic.Int32
	inner := func(ctx context.Context, addr string, chunkID domain.ChunkID) ([]byte, error) {
		calls.Add(1)
		return make([]byte, 32), nil
	}
	wrapped := wrapMaintChecksumVerifier(inner, 0)
	_, err := wrapped(context.Background(), "127.0.0.1:1", domain.ChunkID("c1"))
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls=%d", calls.Load())
	}
}
