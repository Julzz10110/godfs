package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"

	godfsv1 "godfs/api/proto/godfs/v1"
	grpcsvc "godfs/internal/adapter/grpc"
	"godfs/internal/adapter/repository/metadata"
	"godfs/internal/config"
	"godfs/internal/observability"
	"godfs/internal/raftmeta"
	"godfs/internal/security"
	"godfs/internal/usecase"
)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func main() {
	ctx := context.Background()
	shutdownOTel, err := observability.InitOTel(ctx, "godfs-master")
	if err != nil {
		log.Fatalf("otel: %v", err)
	}
	defer func() { _ = shutdownOTel(context.Background()) }()

	observability.EnableGRPCPrometheusHistograms()
	observability.StartMetricsHTTPServer(os.Getenv("GODFS_METRICS_LISTEN"))

	grpcListen := ":9090"
	if v := os.Getenv("GODFS_MASTER_GRPC_LISTEN"); v != "" {
		grpcListen = v
	} else if v := os.Getenv("GODFS_MASTER_LISTEN"); v != "" { // legacy
		grpcListen = v
	}

	raftListen := os.Getenv("GODFS_MASTER_RAFT_LISTEN")
	raftDir := os.Getenv("GODFS_MASTER_RAFT_DIR")
	nodeID := os.Getenv("GODFS_MASTER_NODE_ID")
	peersRaw := os.Getenv("GODFS_MASTER_PEERS")
	bootstrap := strings.EqualFold(os.Getenv("GODFS_MASTER_BOOTSTRAP"), "true") || os.Getenv("GODFS_MASTER_BOOTSTRAP") == "1"
	nodeDeadAfter := 10 * time.Second
	if v := os.Getenv("GODFS_NODE_DEAD_AFTER"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d >= 0 {
			nodeDeadAfter = d
		}
	}
	rebalanceEvery := 5 * time.Second
	if v := os.Getenv("GODFS_REBALANCE_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d >= 0 {
			rebalanceEvery = d
		}
	}
	rebalanceMaxPerTick := 2
	if v := os.Getenv("GODFS_REBALANCE_MAX_PER_TICK"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			rebalanceMaxPerTick = n
		}
	}
	rebalanceMaxAttempts := 10
	if v := os.Getenv("GODFS_REBALANCE_MAX_ATTEMPTS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			rebalanceMaxAttempts = n
		}
	}
	rebalanceBackoffBase := 500 * time.Millisecond
	if v := os.Getenv("GODFS_REBALANCE_BACKOFF_BASE"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			rebalanceBackoffBase = d
		}
	}
	rebalanceBackoffMax := 30 * time.Second
	if v := os.Getenv("GODFS_REBALANCE_BACKOFF_MAX"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			rebalanceBackoffMax = d
		}
	}
	gcEvery := 2 * time.Second
	if v := os.Getenv("GODFS_GC_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d >= 0 {
			gcEvery = d
		}
	}
	gcMaxPerTick := 4
	if v := os.Getenv("GODFS_GC_MAX_PER_TICK"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			gcMaxPerTick = n
		}
	}
	gcMaxAttempts := 20
	if v := os.Getenv("GODFS_GC_MAX_ATTEMPTS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			gcMaxAttempts = n
		}
	}
	gcBaseBackoff := 250 * time.Millisecond
	if v := os.Getenv("GODFS_GC_BACKOFF_BASE"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			gcBaseBackoff = d
		}
	}
	gcMaxBackoff := 10 * time.Second
	if v := os.Getenv("GODFS_GC_BACKOFF_MAX"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			gcMaxBackoff = d
		}
	}
	orphanEvery := 30 * time.Second
	if v := os.Getenv("GODFS_ORPHAN_GC_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d >= 0 {
			orphanEvery = d
		}
	}
	orphanMinAge := 2 * time.Minute
	if v := os.Getenv("GODFS_ORPHAN_GC_MIN_AGE"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d >= 0 {
			orphanMinAge = d
		}
	}
	orphanMaxPerNode := 50
	if v := os.Getenv("GODFS_ORPHAN_GC_MAX_PER_NODE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			orphanMaxPerNode = n
		}
	}

	chunkSize := int64(config.DefaultChunkSize)
	if v := os.Getenv("GODFS_CHUNK_SIZE_BYTES"); v != "" {
		var n int64
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil && n > 0 {
			chunkSize = n
		}
	}
	replication := config.DefaultReplication
	if v := os.Getenv("GODFS_REPLICATION"); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil && n > 0 {
			replication = n
		}
	}

	ln, err := net.Listen("tcp", grpcListen)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	var store usecase.MasterStore
	if raftListen != "" || raftDir != "" || nodeID != "" || peersRaw != "" || bootstrap {
		if raftListen == "" || raftDir == "" || nodeID == "" {
			log.Fatalf("raft mode requires GODFS_MASTER_NODE_ID, GODFS_MASTER_RAFT_LISTEN, GODFS_MASTER_RAFT_DIR")
		}
		peers, grpcByRaft, err := raftmeta.ParsePeers(peersRaw)
		if err != nil {
			log.Fatalf("parse peers: %v", err)
		}
		leaseDur := time.Duration(config.DefaultLeaseSec) * time.Second
		if v := os.Getenv("GODFS_LEASE_SEC"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				leaseDur = time.Duration(n) * time.Second
			}
		}
		node, err := raftmeta.StartNode(raftmeta.NodeConfig{
			NodeID:        nodeID,
			RaftListen:    raftListen,
			RaftDir:       raftDir,
			ChunkSize:     chunkSize,
			Replication:   replication,
			LeaseDur:      leaseDur,
			NodeDeadAfter: nodeDeadAfter,
			Peers:         peers,
			Bootstrap:     bootstrap,
		})
		if err != nil {
			log.Fatalf("start raft: %v", err)
		}
		rstore := raftmeta.NewService(node.Raft, node.FSM, grpcByRaft)
		store = rstore
		log.Printf("goDFS master (raft) grpc=%s raft=%s node=%s peers=%d bootstrap=%v", grpcListen, raftListen, nodeID, len(peers), bootstrap)

		startRaftBackgroundMaintenance(rstore, maintenanceLoopConfig{
			rebalanceEvery:       rebalanceEvery,
			rebalanceMaxPerTick:  rebalanceMaxPerTick,
			rebalanceMaxAttempts: rebalanceMaxAttempts,
			rebalanceBackoffBase: rebalanceBackoffBase,
			rebalanceBackoffMax:  rebalanceBackoffMax,
			gcEvery:              gcEvery,
			gcMaxPerTick:         gcMaxPerTick,
			gcMaxAttempts:        gcMaxAttempts,
			gcBaseBackoff:        gcBaseBackoff,
			gcMaxBackoff:         gcMaxBackoff,
			orphanEvery:          orphanEvery,
			orphanMinAge:         orphanMinAge,
			orphanMaxPerNode:     orphanMaxPerNode,
		})
	} else {
		metaStore := metadata.NewStore(chunkSize, replication)
		metaStore.SetNodeDeadAfter(nodeDeadAfter)
		store = metaStore
		log.Printf("goDFS master (single) grpc=%s (chunk size %d bytes, replication %d)", grpcListen, chunkSize, replication)
		startSingleMasterBackgroundMaintenance(metaStore, maintenanceLoopConfig{
			rebalanceEvery:       rebalanceEvery,
			rebalanceMaxPerTick:  rebalanceMaxPerTick,
			rebalanceMaxAttempts: rebalanceMaxAttempts,
			rebalanceBackoffBase: rebalanceBackoffBase,
			rebalanceBackoffMax:  rebalanceBackoffMax,
			gcEvery:              gcEvery,
			gcMaxPerTick:         gcMaxPerTick,
			gcMaxAttempts:        gcMaxAttempts,
			gcBaseBackoff:        gcBaseBackoff,
			gcMaxBackoff:         gcMaxBackoff,
			orphanEvery:          orphanEvery,
			orphanMinAge:         orphanMinAge,
			orphanMaxPerNode:     orphanMaxPerNode,
		})
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
	auth, err := security.LoadAuthFromEnv()
	if err != nil {
		log.Fatalf("auth config: %v", err)
	}
	rbacJSON := security.RBACRulesJSON()
	rbac, err := security.NewRBAC(rbacJSON, rbacJSON == "")
	if err != nil {
		log.Fatalf("rbac: %v", err)
	}
	audit, err := security.NewAuditLoggerFromEnv()
	if err != nil {
		log.Fatalf("audit: %v", err)
	}
	var unary []grpc.UnaryServerInterceptor
	unary = append(unary, observability.GRPCUnaryPrometheusInterceptor())
	if auth.Enabled {
		unary = append(unary, grpcsvc.NewMasterUnaryInterceptor(auth, rbac, audit))
	}
	serverOpts = append(serverOpts,
		grpc.ChainUnaryInterceptor(unary...),
		grpc.ChainStreamInterceptor(observability.GRPCStreamPrometheusInterceptor()),
	)

	srv := grpc.NewServer(serverOpts...)
	godfsv1.RegisterMasterServiceServer(srv, &grpcsvc.MasterServer{Store: store})
	observability.RegisterGRPCPrometheus(srv)

	if err := srv.Serve(ln); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
