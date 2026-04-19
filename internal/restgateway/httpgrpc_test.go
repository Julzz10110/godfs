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
