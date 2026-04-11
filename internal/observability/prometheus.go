package observability

import (
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"google.golang.org/grpc"
)

// EnableGRPCPrometheusHistograms enables latency histograms for gRPC (call once per process).
func EnableGRPCPrometheusHistograms() {
	grpc_prometheus.EnableHandlingTimeHistogram()
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
