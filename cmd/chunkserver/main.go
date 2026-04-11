package main

import (
	"context"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"

	godfsv1 "godfs/api/proto/godfs/v1"
	grpcsvc "godfs/internal/adapter/grpc"
	chstor "godfs/internal/adapter/repository/chunk"
	"godfs/internal/config"
	"godfs/internal/observability"
	"godfs/internal/security"
)

func main() {
	ctx := context.Background()
	shutdownOTel, err := observability.InitOTel(ctx, "godfs-chunkserver")
	if err != nil {
		log.Fatalf("otel: %v", err)
	}
	defer func() { _ = shutdownOTel(context.Background()) }()

	observability.EnableGRPCPrometheusHistograms()
	observability.StartMetricsHTTPServer(os.Getenv("GODFS_METRICS_LISTEN"))

	listen := ":8000"
	if v := os.Getenv("GODFS_CHUNK_LISTEN"); v != "" {
		listen = v
	}
	dataDir := "./chunkdata"
	if v := os.Getenv("GODFS_CHUNK_DATA"); v != "" {
		dataDir = v
	}
	master := os.Getenv("GODFS_MASTER")
	if master == "" {
		master = "127.0.0.1:9090"
	}
	nodeID := os.Getenv("GODFS_NODE_ID")
	if nodeID == "" {
		nodeID = "chunk-1"
	}
	advertise := os.Getenv("GODFS_ADVERTISE_ADDR")
	if advertise == "" {
		// default: assume same host as client connects to master
		advertise = "127.0.0.1" + listen
	}

	st, err := chstor.NewFSStore(dataDir)
	if err != nil {
		log.Fatalf("storage: %v", err)
	}

	var readCache *chstor.ReadRangeCache
	if v := os.Getenv("GODFS_CHUNK_READ_CACHE_ENTRIES"); v != "" {
		entries, err := strconv.Atoi(v)
		if err == nil && entries > 0 {
			maxB := int64(8 * 1024 * 1024)
			if s := os.Getenv("GODFS_CHUNK_READ_CACHE_MAX_BYTES"); s != "" {
				if x, err := strconv.ParseInt(s, 10, 64); err == nil && x > 0 {
					maxB = x
				}
			}
			readCache, err = chstor.NewReadRangeCache(entries, maxB)
			if err != nil {
				log.Fatalf("read cache: %v", err)
			}
			if readCache != nil {
				log.Printf("chunk read cache: entries=%d max_entry_bytes=%d", entries, maxB)
			}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dopts, err := security.ClientDialOptions()
	if err != nil {
		log.Fatalf("dial options: %v", err)
	}
	conn, err := grpc.NewClient(master, dopts...)
	if err != nil {
		log.Fatalf("dial master: %v", err)
	}
	defer conn.Close()

	mc := godfsv1.NewMasterServiceClient(conn)
	if _, err := mc.RegisterNode(ctx, &godfsv1.RegisterNodeRequest{
		NodeId:        nodeID,
		GrpcAddress:   advertise,
		CapacityBytes: 1 << 40,
	}); err != nil {
		log.Fatalf("register: %v", err)
	}
	log.Printf("registered with master %s as %s @ %s", master, nodeID, advertise)

	hbInterval := 2 * time.Second
	if v := os.Getenv("GODFS_HEARTBEAT_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			hbInterval = d
		}
	}
	go func() {
		t := time.NewTicker(hbInterval)
		defer t.Stop()
		for range t.C {
			hctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, err := mc.Heartbeat(hctx, &godfsv1.HeartbeatRequest{
				NodeId:        nodeID,
				GrpcAddress:   advertise,
				CapacityBytes: 1 << 40,
				UsedBytes:     0,
			})
			cancel()
			if err != nil {
				log.Printf("heartbeat error: %v", err)
			}
		}
	}()

	ln, err := net.Listen("tcp", listen)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	tlsCfg := security.LoadTLSConfigFromEnv()
	var serverOpts []grpc.ServerOption
	serverOpts = append(serverOpts, config.GRPCServerOptions()...)
	serverOpts = observability.PrependOTelStatsHandler(serverOpts)
	if tlsCfg.Enabled {
		if tlsCfg.CertFile == "" || tlsCfg.KeyFile == "" {
			log.Fatal("GODFS_TLS_ENABLED requires GODFS_TLS_CERT_FILE and GODFS_TLS_KEY_FILE")
		}
		creds, err := security.ServerTransportCredentials(tlsCfg)
		if err != nil {
			log.Fatalf("server tls: %v", err)
		}
		serverOpts = append(serverOpts, grpc.Creds(creds))
	}
	clusterKey := os.Getenv("GODFS_CLUSTER_KEY")
	audit, err := security.NewAuditLoggerFromEnv()
	if err != nil {
		log.Fatalf("audit: %v", err)
	}
	chunkAudit := security.ChunkAuditEnabledFromEnv()
	serverOpts = append(serverOpts,
		grpc.ChainUnaryInterceptor(
			observability.GRPCUnaryPrometheusInterceptor(),
			grpcsvc.NewChunkUnaryInterceptor(clusterKey, audit, chunkAudit),
		),
		grpc.ChainStreamInterceptor(
			observability.GRPCStreamPrometheusInterceptor(),
			grpcsvc.NewChunkStreamInterceptor(clusterKey, audit, chunkAudit),
		),
	)
	srv := grpc.NewServer(serverOpts...)
	godfsv1.RegisterChunkServiceServer(srv, &grpcsvc.ChunkServer{Store: st, ReadCache: readCache})
	observability.RegisterGRPCPrometheus(srv)

	log.Printf("chunk server listening on %s", listen)
	if err := srv.Serve(ln); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
