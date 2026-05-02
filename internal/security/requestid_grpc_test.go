package security

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"
)

func TestRequestIDFromIncomingContext(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-request-id", "abc-123"))
	if got := RequestIDFromIncomingContext(ctx); got != "abc-123" {
		t.Fatalf("got %q", got)
	}
	if got := RequestIDFromIncomingContext(context.Background()); got != "" {
		t.Fatalf("want empty got %q", got)
	}
}
