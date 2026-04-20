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
		{h: "bytes=-5", size: 10, ok: false},    // suffix ranges not supported (yet)
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
