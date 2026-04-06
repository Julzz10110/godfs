package main

import (
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
	"godfs/internal/raftmeta"
	"godfs/internal/usecase"
)

func main() {
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
			NodeID:     nodeID,
			RaftListen: raftListen,
			RaftDir:    raftDir,
			ChunkSize:  chunkSize,
			Replication: replication,
			LeaseDur:   leaseDur,
			Peers:      peers,
			Bootstrap:  bootstrap,
		})
		if err != nil {
			log.Fatalf("start raft: %v", err)
		}
		store = raftmeta.NewService(node.Raft, node.FSM, grpcByRaft)
		log.Printf("goDFS master (raft) grpc=%s raft=%s node=%s peers=%d bootstrap=%v", grpcListen, raftListen, nodeID, len(peers), bootstrap)
	} else {
		store = metadata.NewStore(chunkSize, replication)
		log.Printf("goDFS master (single) grpc=%s (chunk size %d bytes, replication %d)", grpcListen, chunkSize, replication)
	}

	srv := grpc.NewServer()
	godfsv1.RegisterMasterServiceServer(srv, &grpcsvc.MasterServer{Store: store})

	if err := srv.Serve(ln); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
