package chunk

import (
	"errors"
	"testing"

	"godfs/internal/domain"
)

func TestFSStoreWriteReadRoundTrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewFSStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	const id = "abc-def-001"
	n, sum, err := s.WriteAt(id, 0, []byte("hello"))
	if err != nil || n != 5 || len(sum) != 32 {
		t.Fatalf("write: n=%d sum=%d err=%v", n, len(sum), err)
	}
	buf := make([]byte, 16)
	rn, err := s.ReadAt(id, 0, buf)
	if err != nil || rn != 5 || string(buf[:rn]) != "hello" {
		t.Fatalf("read: rn=%d data=%q err=%v", rn, buf[:rn], err)
	}
	if sz, err := s.Size(id); err != nil || sz != 5 {
		t.Fatalf("size: %d err=%v", sz, err)
	}
	ids, err := s.ListChunkIDs()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, x := range ids {
		if x == id {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("list ids: %v", ids)
	}
}

func TestFSStoreRejectBadChunkIDs(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewFSStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	bad := []string{
		"",
		".",
		"..",
		"x/y",
		"../escape",
	}
	for _, id := range bad {
		_, _, err := s.WriteAt(id, 0, []byte("x"))
		if !errors.Is(err, domain.ErrInvalidPath) {
			t.Fatalf("WriteAt(%q): got %v want %v", id, err, domain.ErrInvalidPath)
		}
	}
}
