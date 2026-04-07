package main

import (
	"context"
	"log"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	godfsv1 "godfs/api/proto/godfs/v1"
	grpcsvc "godfs/internal/adapter/grpc"
	chstor "godfs/internal/adapter/repository/chunk"
)

func main() {
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(master, grpc.WithTransportCredentials(insecure.NewCredentials()))
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

	srv := grpc.NewServer()
	godfsv1.RegisterChunkServiceServer(srv, &grpcsvc.ChunkServer{Store: st})

	log.Printf("chunk server listening on %s", listen)
	if err := srv.Serve(ln); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
