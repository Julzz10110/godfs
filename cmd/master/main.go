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
	"google.golang.org/grpc/credentials/insecure"

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
	gcEvery := 2 * time.Second
	if v := os.Getenv("GODFS_GC_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d >= 0 {
			gcEvery = d
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
			NodeID:     nodeID,
			RaftListen: raftListen,
			RaftDir:    raftDir,
			ChunkSize:  chunkSize,
			Replication: replication,
			LeaseDur:   leaseDur,
			NodeDeadAfter: nodeDeadAfter,
			Peers:      peers,
			Bootstrap:  bootstrap,
		})
		if err != nil {
			log.Fatalf("start raft: %v", err)
		}
		rstore := raftmeta.NewService(node.Raft, node.FSM, grpcByRaft)
		store = rstore
		log.Printf("goDFS master (raft) grpc=%s raft=%s node=%s peers=%d bootstrap=%v", grpcListen, raftListen, nodeID, len(peers), bootstrap)

		if rebalanceEvery > 0 {
			go func() {
				t := time.NewTicker(rebalanceEvery)
				defer t.Stop()
				for range t.C {
					if !rstore.IsLeader() {
						continue
					}
					act, err := rstore.PlanRebalance(time.Now().UTC())
					if err != nil || act == nil {
						continue
					}
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					_ = rstore.ExecuteRebalance(ctx, act)
					cancel()
				}
			}()
		}

		if gcEvery > 0 {
			go func() {
				t := time.NewTicker(gcEvery)
				defer t.Stop()
				for range t.C {
					if !rstore.IsLeader() {
						continue
					}
					cid, addr, ok := rstore.PlanDeleteGC()
					if !ok {
						continue
					}
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					err := func() error {
						cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
						if err != nil {
							return err
						}
						defer cc.Close()
						cli := godfsv1.NewChunkServiceClient(cc)
						_, err = cli.DeleteChunk(ctx, &godfsv1.DeleteChunkRequest{ChunkId: string(cid)})
						return err
					}()
					cancel()
					if err != nil {
						continue
					}
					uctx, ucancel := context.WithTimeout(context.Background(), 5*time.Second)
					_ = rstore.ClearPendingDeleteAddr(uctx, cid, addr)
					ucancel()
				}
			}()
		}
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
