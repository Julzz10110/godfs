package security

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
)

// GRPCUnaryRequestIDSpanInterceptor adds request_id to the active span when x-request-id is in metadata.
func GRPCUnaryRequestIDSpanInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		setRequestIDSpanAttr(ctx)
		return handler(ctx, req)
	}
}

// GRPCStreamRequestIDSpanInterceptor is the streaming counterpart for ChunkService RPCs.
func GRPCStreamRequestIDSpanInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		setRequestIDSpanAttr(ss.Context())
		return handler(srv, ss)
	}
}

func setRequestIDSpanAttr(ctx context.Context) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	if rid := RequestIDFromIncomingContext(ctx); rid != "" {
		span.SetAttributes(attribute.String("request_id", rid))
	}
}
