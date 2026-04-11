package observability

import (
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

// GRPCClientDialOptions adds OpenTelemetry client stats propagation when OTLP tracing is enabled.
func GRPCClientDialOptions() []grpc.DialOption {
	if !OTelTraceConfigured() {
		return nil
	}
	return []grpc.DialOption{grpc.WithStatsHandler(otelgrpc.NewClientHandler())}
}
