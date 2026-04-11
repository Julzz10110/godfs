package client

import (
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsRetryableGRPC(t *testing.T) {
	if isRetryableGRPC(nil) {
		t.Fatal("nil not retryable")
	}
	if !isRetryableGRPC(status.Error(codes.Unavailable, "x")) {
		t.Fatal("Unavailable retryable")
	}
	if isRetryableGRPC(status.Error(codes.InvalidArgument, "x")) {
		t.Fatal("InvalidArgument not retryable")
	}
	if isRetryableGRPC(errors.New("plain")) {
		t.Fatal("plain error not retryable")
	}
}
