package restgateway

import (
	"errors"
	"net/http"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestStatusFromGRPC(t *testing.T) {
	tests := []struct {
		code codes.Code
		want int
	}{
		{codes.NotFound, http.StatusNotFound},
		{codes.AlreadyExists, http.StatusConflict},
		{codes.InvalidArgument, http.StatusBadRequest},
		{codes.Unauthenticated, http.StatusUnauthorized},
		{codes.PermissionDenied, http.StatusForbidden},
		{codes.Unavailable, http.StatusServiceUnavailable},
	}
	for _, tt := range tests {
		err := status.Error(tt.code, "x")
		if got := statusFromGRPC(err); got != tt.want {
			t.Errorf("%v: got %d want %d", tt.code, got, tt.want)
		}
	}
	if got := statusFromGRPC(errors.New("plain")); got != http.StatusInternalServerError {
		t.Errorf("non-grpc: got %d", got)
	}
}

func TestRequirePath(t *testing.T) {
	if _, ok := requirePath(""); ok {
		t.Fatal("empty")
	}
	if _, ok := requirePath("rel"); ok {
		t.Fatal("relative")
	}
	p, ok := requirePath("/a")
	if !ok || p != "/a" {
		t.Fatal("/a")
	}
}

func TestParseSingleByteRange(t *testing.T) {
	type tc struct {
		h    string
		size int64
		ok   bool
		s    int64
		e    int64
	}
	tests := []tc{
		{h: "bytes=0-0", size: 10, ok: true, s: 0, e: 1},
		{h: "bytes=0-9", size: 10, ok: true, s: 0, e: 10},
		{h: "bytes=1-", size: 10, ok: true, s: 1, e: 10},
		{h: "bytes=9-", size: 10, ok: true, s: 9, e: 10},
		{h: "bytes=10-", size: 10, ok: false},
		{h: "bytes=5-4", size: 10, ok: false},
		{h: "bytes=-5", size: 10, ok: true, s: 5, e: 10},
		{h: "bytes=-100", size: 10, ok: true, s: 0, e: 10},
		{h: "bytes=-0", size: 10, ok: false},
		{h: "bytes=-5", size: 0, ok: false},
		{h: "bytes=0-100", size: 10, ok: true, s: 0, e: 10},
		{h: "bytes=0-", size: 0, ok: false},
		{h: "", size: 10, ok: false},
	}
	for _, tt := range tests {
		s, e, ok := parseSingleByteRange(tt.h, tt.size)
		if ok != tt.ok {
			t.Fatalf("%q size=%d: ok=%v want %v", tt.h, tt.size, ok, tt.ok)
		}
		if ok && (s != tt.s || e != tt.e) {
			t.Fatalf("%q size=%d: got [%d,%d) want [%d,%d)", tt.h, tt.size, s, e, tt.s, tt.e)
		}
	}
}

func TestParseMultiByteRanges(t *testing.T) {
	rs, ok := parseMultiByteRanges("bytes=0-0, 2-3, -2", 10, 16)
	if !ok {
		t.Fatal("expected ok")
	}
	if len(rs) != 3 {
		t.Fatalf("len=%d", len(rs))
	}
	if rs[0].start != 0 || rs[0].end != 1 {
		t.Fatalf("r0=%+v", rs[0])
	}
	if rs[1].start != 2 || rs[1].end != 4 {
		t.Fatalf("r1=%+v", rs[1])
	}
	if rs[2].start != 8 || rs[2].end != 10 {
		t.Fatalf("r2=%+v", rs[2])
	}

	if _, ok := parseMultiByteRanges("bytes=0-0,", 10, 16); ok {
		t.Fatal("trailing comma should fail")
	}
	if _, ok := parseMultiByteRanges("items=0-0", 10, 16); ok {
		t.Fatal("wrong unit")
	}
	if _, ok := parseMultiByteRanges("bytes=0-0,1-1", 10, 1); ok {
		t.Fatal("maxRanges exceeded")
	}
}
