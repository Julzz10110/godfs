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
	"godfs/internal/raftmeta"
	"godfs/pkg/client"
)

func TestE2E_OrphanGC_RemovesUnknownChunks(t *testing.T) {
	const chunkSize = 32 * 1024
	const replication = 1

	base := t.TempDir()

	raftLn, _ := net.Listen("tcp", "127.0.0.1:0")
	raftAddr := raftLn.Addr().String()
	_ = raftLn.Close()
	node, err := raftmeta.StartNode(raftmeta.NodeConfig{
		NodeID:        "m0",
		RaftListen:    raftAddr,
		RaftDir:       filepath.Join(base, "raft"),
		ChunkSize:     chunkSize,
		Replication:   replication,
		NodeDeadAfter: 0,
		Peers:         nil,
		Bootstrap:     true,
	})
	if err != nil {
		t.Fatal(err)
	}
	store := raftmeta.NewService(node.Raft, node.FSM, map[string]string{raftAddr: "127.0.0.1:0"})

	mLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mSrv := grpc.NewServer()
	godfsv1.RegisterMasterServiceServer(mSrv, &grpcsvc.MasterServer{Store: store})
	go func() { _ = mSrv.Serve(mLn) }()
	defer func() { mSrv.Stop(); _ = mLn.Close(); _ = node.Close() }()

	// wait leader
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if store.IsLeader() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !store.IsLeader() {
		t.Fatal("no leader")
	}

	chDir := filepath.Join(base, "chunk")
	_ = os.MkdirAll(chDir, 0o750)
	st, err := chstor.NewFSStore(chDir)
	if err != nil {
		t.Fatal(err)
	}
	chLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	chAddr := chLn.Addr().String()
	chSrv := grpc.NewServer()
	godfsv1.RegisterChunkServiceServer(chSrv, &grpcsvc.ChunkServer{Store: st})
	go func() { _ = chSrv.Serve(chLn) }()
	defer func() { chSrv.Stop(); _ = chLn.Close() }()

	// Register node in master.
	mConn, err := grpc.NewClient(mLn.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer mConn.Close()
	mCli := godfsv1.NewMasterServiceClient(mConn)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := mCli.RegisterNode(ctx, &godfsv1.RegisterNodeRequest{NodeId: "n1", GrpcAddress: chAddr, CapacityBytes: 1 << 30}); err != nil {
		t.Fatal(err)
	}

	cli, err := client.NewWithChunkSize(mLn.Addr().String(), chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	if err := cli.Mkdir(ctx, "/o"); err != nil {
		t.Fatal(err)
	}
	if err := cli.Create(ctx, "/o/x"); err != nil {
		t.Fatal(err)
	}
	if err := cli.Write(ctx, "/o/x", []byte("ok")); err != nil {
		t.Fatal(err)
	}

	// Create an orphan chunk file on the chunkserver disk.
	orphanPath := filepath.Join(chDir, "orphan.chk")
	if err := os.WriteFile(orphanPath, []byte("orphan"), 0o640); err != nil {
		t.Fatal(err)
	}

	// Run orphan GC once.
	gctx, gcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer gcancel()
	if err := store.OrphanGCOnce(gctx); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(orphanPath); err == nil {
		t.Fatalf("expected orphan chunk to be deleted: %s", orphanPath)
	}
}

