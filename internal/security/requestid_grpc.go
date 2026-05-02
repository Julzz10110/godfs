package security

import (
	"context"
	"strings"

	"google.golang.org/grpc/metadata"
)

// RequestIDFromIncomingContext returns the first x-request-id value from incoming gRPC metadata.
func RequestIDFromIncomingContext(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	if v := md.Get("x-request-id"); len(v) > 0 {
		return strings.TrimSpace(v[0])
	}
	return ""
}
