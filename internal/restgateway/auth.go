package restgateway

import (
	"context"
	"net/http"
	"os"
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
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if auth == "" && strings.HasPrefix(r.URL.Path, "/v1/fs/content") {
		if p, ok := requirePath(r.URL.Query().Get("path")); ok && presignedGETValid(r, p) {
			if ub := strings.TrimSpace(os.Getenv("GODFS_REST_PRESIGN_UPSTREAM_BEARER")); ub != "" {
				auth = "Bearer " + ub
			}
		}
	}
	ctx = WithBearerAuth(ctx, auth)
	if id := strings.TrimSpace(RequestIDFromContext(ctx)); id != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-request-id", id)
	}
	return ctx
}
