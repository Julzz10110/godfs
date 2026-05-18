package observability

import (
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"google.golang.org/grpc"
)

// grpcHistogramBuckets supports p95/p99 SLO recording (up to 30s for large chunk RPCs).
var grpcHistogramBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}

// EnableGRPCPrometheusHistograms enables latency histograms for gRPC (call once per process).
func EnableGRPCPrometheusHistograms() {
	grpc_prometheus.EnableHandlingTimeHistogram(
		grpc_prometheus.WithHistogramBuckets(grpcHistogramBuckets),
	)
}

// GRPCUnaryPrometheusInterceptor records unary RPC metrics.
func GRPCUnaryPrometheusInterceptor() grpc.UnaryServerInterceptor {
	return grpc_prometheus.UnaryServerInterceptor
}

// GRPCStreamPrometheusInterceptor records streaming RPC metrics.
func GRPCStreamPrometheusInterceptor() grpc.StreamServerInterceptor {
	return grpc_prometheus.StreamServerInterceptor
}

// RegisterGRPCPrometheus registers server metrics collectors (call after Register*Server).
func RegisterGRPCPrometheus(s *grpc.Server) {
	grpc_prometheus.Register(s)
}
