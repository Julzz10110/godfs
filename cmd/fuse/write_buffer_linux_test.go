//go:build linux

package main

import (
	"context"
	"testing"
)

func TestMergeWriteRanges_adjacent(t *testing.T) {
	in := []writeRange{
		{off: 0, data: []byte("ab")},
		{off: 2, data: []byte("cd")},
	}
	out := mergeWriteRanges(in)
	if len(out) != 1 || string(out[0].data) != "abcd" {
		t.Fatalf("got %+v", out)
	}
}

func TestFuseWriteBuffer_flushInvokesWriteAt(t *testing.T) {
	var b fuseWriteBuffer
	b.write(10, []byte("hi"))
	var calls int
	err := b.flush(context.Background(), "/p", func(_ context.Context, _ string, off int64, data []byte) error {
		calls++
		if off != 10 || string(data) != "hi" {
			t.Fatalf("off=%d data=%q", off, data)
		}
		return nil
	})
	if err != nil || calls != 1 || !b.empty() {
		t.Fatalf("err=%v calls=%d empty=%v", err, calls, b.empty())
	}
}

func TestFuseWriteBuffer_readAt(t *testing.T) {
	var b fuseWriteBuffer
	b.write(5, []byte("xyz"))
	n, ok := b.readAt(make([]byte, 2), 6)
	if !ok || n != 2 || string(b.ranges[0].data) == "" {
		t.Fatalf("n=%d ok=%v", n, ok)
	}
	dst := make([]byte, 2)
	n, ok = b.readAt(dst, 6)
	if !ok || n != 2 || string(dst) != "yz" {
		t.Fatalf("dst=%q n=%d ok=%v", dst, n, ok)
	}
}
