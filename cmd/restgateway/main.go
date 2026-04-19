package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strconv"

	"godfs/internal/config"
	"godfs/internal/observability"
	"godfs/internal/restgateway"
	"godfs/pkg/client"
)

func main() {
	ctx := context.Background()
	shutdownOTel, err := observability.InitOTel(ctx, "godfs-restgateway")
	if err != nil {
		log.Fatalf("otel: %v", err)
	}
	defer func() { _ = shutdownOTel(context.Background()) }()

	observability.EnableGRPCPrometheusHistograms()
	observability.StartMetricsHTTPServer(os.Getenv("GODFS_METRICS_LISTEN"))

	master := os.Getenv("GODFS_MASTER")
	if master == "" {
		master = "127.0.0.1:9090"
	}
	listen := ":8080"
	if v := os.Getenv("GODFS_REST_LISTEN"); v != "" {
		listen = v
	}
	chunkSize := int64(config.DefaultChunkSize)
	if v := os.Getenv("GODFS_CHUNK_SIZE_BYTES"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			chunkSize = n
		}
	}

	cli, err := client.NewGateway(master, chunkSize)
	if err != nil {
		log.Fatalf("client: %v", err)
	}
	defer cli.Close()

	srv := &restgateway.Server{
		Client:  cli,
		MaxBody: restgateway.DefaultMaxBodyBytes(),
	}
	mux := http.NewServeMux()
	srv.Register(mux)

	log.Printf("goDFS REST gateway listening on %s (master gRPC %s)", listen, master)
	if err := http.ListenAndServe(listen, mux); err != nil {
		log.Fatal(err)
	}
}
