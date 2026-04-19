package restgateway

import (
	"context"
	"strings"

	"google.golang.org/grpc/metadata"
)

// WithBearerAuth appends gRPC outgoing metadata from the HTTP Authorization header (Bearer token / JWT).
func WithBearerAuth(ctx context.Context, authorizationHeader string) context.Context {
	h := strings.TrimSpace(authorizationHeader)
	if h == "" {
		return ctx
	}
	if !strings.HasPrefix(strings.ToLower(h), "bearer ") {
		h = "Bearer " + h
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", h)
}
