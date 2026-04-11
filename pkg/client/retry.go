package client

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func isRetryableGRPC(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch st.Code() {
	case codes.Unavailable, codes.ResourceExhausted, codes.Aborted, codes.DeadlineExceeded:
		return true
	default:
		return false
}
}

// grpcRetry runs fn up to attempts times with exponential backoff on retryable gRPC errors.
func grpcRetry(ctx context.Context, attempts int, fn func() error) error {
	if attempts < 1 {
		attempts = 1
	}
	var last error
	backoff := 50 * time.Millisecond
	for i := 0; i < attempts; i++ {
		last = fn()
		if last == nil {
			return nil
		}
		if !isRetryableGRPC(last) || i == attempts-1 {
			return last
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 2*time.Second {
			backoff *= 2
		}
	}
	return last
}
