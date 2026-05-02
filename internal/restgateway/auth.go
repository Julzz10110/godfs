package restgateway

import (
	"context"
	"net/http"
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

// OutgoingRPCContext builds a context for outbound gRPC from an HTTP request: Bearer auth and x-request-id (when set by [WithRequestID]).
func OutgoingRPCContext(r *http.Request) context.Context {
	ctx := r.Context()
	ctx = WithBearerAuth(ctx, r.Header.Get("Authorization"))
	if id := strings.TrimSpace(RequestIDFromContext(ctx)); id != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-request-id", id)
	}
	return ctx
}
