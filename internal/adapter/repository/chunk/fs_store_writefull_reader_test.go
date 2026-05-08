package chunk

import (
	"bytes"
	"strings"
	"testing"
)

func TestFSStore_WriteFullFromReader_ReplacesContents(t *testing.T) {
	dir := t.TempDir()
	st, err := NewFSStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	if err := st.WriteFull("c1", []byte("old")); err != nil {
		t.Fatal(err)
	}
	n, err := st.WriteFullFromReader("c1", strings.NewReader("newdata"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len("newdata")) {
		t.Fatalf("written=%d", n)
	}
	b, err := st.ReadAll("c1")
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "newdata" {
		t.Fatalf("got %q", string(b))
	}
}

func TestFSStore_WriteFullFromReader_EnforcesMaxBytes(t *testing.T) {
	dir := t.TempDir()
	st, err := NewFSStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, err = st.WriteFullFromReader("c1", bytes.NewReader([]byte("0123456789")), 5)
	if err == nil {
		t.Fatal("expected error")
	}
}

