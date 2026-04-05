package main

import (
	"fmt"
	"log"
	"net"
	"os"

	"google.golang.org/grpc"

	godfsv1 "godfs/api/proto/godfs/v1"
	grpcsvc "godfs/internal/adapter/grpc"
	"godfs/internal/adapter/repository/metadata"
	"godfs/internal/config"
)

func main() {
	addr := ":9090"
	if v := os.Getenv("GODFS_MASTER_LISTEN"); v != "" {
		addr = v
	}
	chunkSize := int64(config.DefaultChunkSize)
	if v := os.Getenv("GODFS_CHUNK_SIZE_BYTES"); v != "" {
		var n int64
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil && n > 0 {
			chunkSize = n
		}
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	store := metadata.NewStore(chunkSize)
	srv := grpc.NewServer()
	godfsv1.RegisterMasterServiceServer(srv, &grpcsvc.MasterServer{Store: store})

	log.Printf("goDFS master listening on %s (chunk size %d bytes)", addr, chunkSize)
	if err := srv.Serve(ln); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
