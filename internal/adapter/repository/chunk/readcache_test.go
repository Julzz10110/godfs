package chunk

import (
	"bytes"
	"testing"
)

func TestReadRangeCacheHitMiss(t *testing.T) {
	c, err := NewReadRangeCache(8, 1024)
	if err != nil || c == nil {
		t.Fatalf("NewReadRangeCache: %v", err)
	}
	if c.MaxEntryBytes() != 1024 {
		t.Fatalf("MaxEntryBytes: %d", c.MaxEntryBytes())
	}
	_, ok := c.Get("cid", 0, 10)
	if ok {
		t.Fatal("expected miss before Add")
	}
	payload := bytes.Repeat([]byte("a"), 10)
	c.Add("cid", 0, 10, payload)
	got, ok := c.Get("cid", 0, 10)
	if !ok || !bytes.Equal(got, payload) {
		t.Fatalf("get: ok=%v len=%d", ok, len(got))
	}
}

func TestReadRangeCacheNilSafe(t *testing.T) {
	var c *ReadRangeCache
	if c.MaxEntryBytes() != 0 {
		t.Fatal("nil MaxEntryBytes")
	}
	_, ok := c.Get("x", 0, 1)
	if ok {
		t.Fatal("nil get")
	}
	c.Add("x", 0, 1, []byte{1}) // no-op
}

func TestReadRangeCacheRejectsBadLength(t *testing.T) {
	c, err := NewReadRangeCache(4, 100)
	if err != nil || c == nil {
		t.Fatal(err)
	}
	c.Add("id", 0, 5, []byte{1, 2, 3}) // len != length
	_, ok := c.Get("id", 0, 5)
	if ok {
		t.Fatal("expected miss for length mismatch")
	}
	c.Add("id", 0, 3, []byte{1, 2, 3})
	_, ok = c.Get("id", 0, 100) // length > maxBytes
	if ok {
		t.Fatal("expected miss for length over max")
	}
}

func TestNewReadRangeCacheInvalidReturnsNil(t *testing.T) {
	c, err := NewReadRangeCache(0, 100)
	if err != nil || c != nil {
		t.Fatalf("entries=0: c=%v err=%v", c, err)
	}
	c2, err := NewReadRangeCache(4, 0)
	if err != nil || c2 != nil {
		t.Fatalf("maxBytes=0: c=%v err=%v", c2, err)
	}
}
