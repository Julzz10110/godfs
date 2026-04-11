package observability

import (
	"context"
	"os"
	"strings"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"google.golang.org/grpc"
)

// OTelTraceConfigured reports whether OTLP trace export should be enabled.
func OTelTraceConfigured() bool {
	return strings.TrimSpace(otlpEndpoint()) != ""
}

func otlpEndpoint() string {
	if v := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")); v != "" {
		return v
	}
	return strings.TrimSpace(os.Getenv("GODFS_OTEL_EXPORTER_OTLP_ENDPOINT"))
}

// normalizeOTLPEndpoint strips a scheme for the gRPC OTLP client (host:port).
func normalizeOTLPEndpoint(ep string) string {
	ep = strings.TrimSpace(ep)
	ep = strings.TrimPrefix(ep, "http://")
	ep = strings.TrimPrefix(ep, "https://")
	return ep
}

func otelServiceName(defaultName string) string {
	if v := strings.TrimSpace(os.Getenv("OTEL_SERVICE_NAME")); v != "" {
		return v
	}
	if v := strings.TrimSpace(os.Getenv("GODFS_OTEL_SERVICE_NAME")); v != "" {
		return v
	}
	if defaultName != "" {
		return defaultName
	}
	return "godfs"
}

// InitOTel configures the global tracer provider when an OTLP endpoint is set.
// Returns a shutdown function (no-op if tracing is disabled).
func InitOTel(ctx context.Context, defaultServiceName string) (func(context.Context) error, error) {
	ep := otlpEndpoint()
	if ep == "" {
		return func(context.Context) error { return nil }, nil
	}
	name := otelServiceName(defaultServiceName)

	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(normalizeOTLPEndpoint(ep)),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}
	res, err := sdkresource.New(ctx,
		sdkresource.WithAttributes(semconv.ServiceName(name)),
	)
	if err != nil {
		_ = exp.Shutdown(ctx)
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	return tp.Shutdown, nil
}

// PrependOTelStatsHandler prepends OpenTelemetry gRPC stats when OTLP is configured.
func PrependOTelStatsHandler(opts []grpc.ServerOption) []grpc.ServerOption {
	if !OTelTraceConfigured() {
		return opts
	}
	return append([]grpc.ServerOption{grpc.StatsHandler(otelgrpc.NewServerHandler())}, opts...)
}
