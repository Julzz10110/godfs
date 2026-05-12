package metadata

import (
	"context"
	"testing"
)

func TestNamespaceSnapshot(t *testing.T) {
	s := NewStore(4096, 2)
	_ = s.Mkdir(context.Background(), "/d")
	_, err := s.CreateFile(context.Background(), "/d/f")
	if err != nil {
		t.Fatal(err)
	}
	f, d, c, b := s.NamespaceSnapshot()
	if d < 1 || f < 1 {
		t.Fatalf("dirs=%d files=%d", d, f)
	}
	if c != 0 || b != 0 {
		t.Fatalf("chunks=%d bytes=%d", c, b)
	}
}
