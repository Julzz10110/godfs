package security

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestGRPCUnaryRequestIDSpanInterceptor_setsAttribute(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(trace.WithSyncer(exp))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	tr := tp.Tracer("test")
	ctx, span := tr.Start(context.Background(), "rpc")
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("x-request-id", "req-abc"))

	ic := GRPCUnaryRequestIDSpanInterceptor()
	_, err := ic(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/test"}, func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, nil
	})
	span.End()
	if err != nil {
		t.Fatal(err)
	}

	for _, s := range exp.GetSpans() {
		for _, a := range s.Attributes {
			if a.Key == "request_id" && a.Value.AsString() == "req-abc" {
				return
			}
		}
	}
	t.Fatalf("request_id not found in exported spans: %+v", exp.GetSpans())
}
