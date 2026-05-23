package e2e_test

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	godfsv1 "godfs/api/proto/godfs/v1"
	grpcsvc "godfs/internal/adapter/grpc"
	chstor "godfs/internal/adapter/repository/chunk"
	"godfs/pkg/client"
	"godfs/test/e2e"
)

// After a chunk stops heartbeating, ListUnderReplicatedChunks is non-empty.
func TestE2E_ListUnderReplicatedChunks_deadNode(t *testing.T) {
	const chunkSize = 32 * 1024
	const replication = 2
	const deadAfter = 500 * time.Millisecond

	base := t.TempDir()
	masterAddr, _ := startSingleRaftMaster(t, "127.0.0.1:0", "127.0.0.1:0", filepath.Join(base, "raft"), chunkSize, replication, deadAfter)

	chunkBase := filepath.Join(base, "chunks")
	_ = os.MkdirAll(chunkBase, 0o750)

	addChunk := func(id string) (stop func()) {
		dir := filepath.Join(chunkBase, id)
		_ = os.MkdirAll(dir, 0o750)
		st, err := chstor.NewFSStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		srv := grpc.NewServer()
		godfsv1.RegisterChunkServiceServer(srv, &grpcsvc.ChunkServer{Store: st})
		go func() { _ = srv.Serve(ln) }()

		conn, err := grpc.NewClient(masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		mc := godfsv1.NewMasterServiceClient(conn)
		rctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		addr := ln.Addr().String()
		if _, err := mc.RegisterNode(rctx, &godfsv1.RegisterNodeRequest{NodeId: id, GrpcAddress: addr, CapacityBytes: 1 << 30}); err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
		_, _ = mc.Heartbeat(rctx, &godfsv1.HeartbeatRequest{NodeId: id, CapacityBytes: 1 << 30, UsedBytes: 0})
		return func() { srv.Stop(); _ = ln.Close() }
	}

	stopA := addChunk("a")
	stopB := addChunk("b")
	defer stopA()
	defer stopB()

	// Heartbeat only node a so b goes dead after deadAfter.
	hbConn, err := grpc.NewClient(masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = hbConn.Close() }()
	hb := godfsv1.NewMasterServiceClient(hbConn)
	done := make(chan struct{})
	go func() {
		tk := time.NewTicker(100 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-done:
				return
			case <-tk.C:
				hctx, cancel := context.WithTimeout(context.Background(), time.Second)
				_, _ = hb.Heartbeat(hctx, &godfsv1.HeartbeatRequest{NodeId: "a", CapacityBytes: 1 << 30, UsedBytes: 0})
				cancel()
			}
		}
	}()
	defer close(done)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cli, err := client.NewWithChunkSize(masterAddr, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	if err := cli.Mkdir(ctx, "/ur"); err != nil {
		t.Fatal(err)
	}
	if err := cli.Create(ctx, "/ur/f.bin"); err != nil {
		t.Fatal(err)
	}
	if err := cli.Write(ctx, "/ur/f.bin", []byte("under-replicated-smoke")); err != nil {
		t.Fatal(err)
	}

	time.Sleep(deadAfter + 400*time.Millisecond)

	entries, total, err := cli.ListUnderReplicatedChunks(ctx, 0)
	if err != nil {
		t.Fatalf("ListUnderReplicatedChunks: %v", err)
	}
	if total < 1 || len(entries) < 1 {
		t.Fatalf("expected under-replicated chunks, total=%d len=%d", total, len(entries))
	}
}

func TestE2E_ListUnderReplicatedChunks_healthyEmpty(t *testing.T) {
	const chunkSize = 32 * 1024
	_, cl := e2e.StartMaster(t, chunkSize, 1)
	dir := t.TempDir()
	cl.AddChunkServer(t, "c0", filepath.Join(dir, "c0"))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cli, err := client.NewWithChunkSize(cl.MasterAddr, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	entries, total, err := cli.ListUnderReplicatedChunks(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 0 || len(entries) != 0 {
		t.Fatalf("expected empty, total=%d entries=%d", total, len(entries))
	}
}
