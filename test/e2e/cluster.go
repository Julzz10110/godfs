package e2e

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	godfsv1 "godfs/api/proto/godfs/v1"
	grpcsvc "godfs/internal/adapter/grpc"
	chstor "godfs/internal/adapter/repository/chunk"
	"godfs/internal/adapter/repository/metadata"
	"godfs/internal/config"
)

// Cluster holds running Master + ChunkServers for e2e tests.
type Cluster struct {
	MasterAddr string
	ChunkDirs  []string

	chunkSrvs []*grpc.Server
	chunkLns  []net.Listener
}

// StartMaster starts Master on 127.0.0.1:0 with given chunk size and replication factor.
func StartMaster(t testing.TB, chunkSize int64, replication int) (*metadata.Store, *Cluster) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen master: %v", err)
	}
	store := metadata.NewStore(chunkSize, replication)
	srv := grpc.NewServer(config.GRPCServerOptions()...)
	godfsv1.RegisterMasterServiceServer(srv, &grpcsvc.MasterServer{Store: store})

	go func() {
		if err := srv.Serve(ln); err != nil {
			t.Logf("master serve: %v", err)
		}
	}()

	t.Cleanup(func() {
		srv.Stop()
		_ = ln.Close()
	})

	c := &Cluster{MasterAddr: ln.Addr().String()}
	return store, c
}

// AddChunkServer starts a ChunkServer on 127.0.0.1:0 and registers it with Master.
func (c *Cluster) AddChunkServer(t testing.TB, nodeID, dataDir string) string {
	t.Helper()

	st, err := chstor.NewFSStore(dataDir)
	if err != nil {
		t.Fatalf("chunk store: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen chunk: %v", err)
	}
	advertise := ln.Addr().String()

	srv := grpc.NewServer(config.GRPCServerOptions()...)
	godfsv1.RegisterChunkServiceServer(srv, &grpcsvc.ChunkServer{Store: st})

	go func() {
		if err := srv.Serve(ln); err != nil {
			t.Logf("chunk %s serve: %v", nodeID, err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dopts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	dopts = append(dopts, config.GRPCDialOptions()...)
	conn, err := grpc.NewClient(c.MasterAddr, dopts...)
	if err != nil {
		t.Fatalf("dial master: %v", err)
	}
	defer conn.Close()
	mc := godfsv1.NewMasterServiceClient(conn)
	if _, err := mc.RegisterNode(ctx, &godfsv1.RegisterNodeRequest{
		NodeId:        nodeID,
		GrpcAddress:   advertise,
		CapacityBytes: 1 << 30,
	}); err != nil {
		t.Fatalf("register node %s: %v", nodeID, err)
	}

	c.ChunkDirs = append(c.ChunkDirs, dataDir)
	c.chunkSrvs = append(c.chunkSrvs, srv)
	c.chunkLns = append(c.chunkLns, ln)

	t.Cleanup(func() {
		srv.Stop()
		_ = ln.Close()
	})

	return advertise
}

// StopChunkServer stops one chunk gRPC server (failure simulation). Index matches registration order in AddChunkServer.
func (c *Cluster) StopChunkServer(t testing.TB, index int) {
	t.Helper()
	if c == nil || index < 0 || index >= len(c.chunkSrvs) {
		t.Fatalf("invalid chunk index %d (servers=%d)", index, len(c.chunkSrvs))
	}
	c.chunkSrvs[index].Stop()
	_ = c.chunkLns[index].Close()
}
