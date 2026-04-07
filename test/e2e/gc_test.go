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

func TestE2E_DeleteGC_RetriesUntilPeerDeleted(t *testing.T) {
	const chunkSize = 32 * 1024
	const replication = 1

	base := t.TempDir()

	// Single-node raft master with short GC interval (manual loop, not cmd/master).
	raftLn, _ := net.Listen("tcp", "127.0.0.1:0")
	raftAddr := raftLn.Addr().String()
	_ = raftLn.Close()
	node, err := raftmeta.StartNode(raftmeta.NodeConfig{
		NodeID:       "m0",
		RaftListen:   raftAddr,
		RaftDir:      filepath.Join(base, "raft"),
		ChunkSize:    chunkSize,
		Replication:  replication,
		NodeDeadAfter: 0,
		Peers:        nil,
		Bootstrap:    true,
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

	// Start a chunk server but keep it stopped initially to force delete retry.
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
	// Do not Serve yet.

	// Register node address in master (so replicas contain it).
	mConn, err := grpc.NewClient(mLn.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	mCli := godfsv1.NewMasterServiceClient(mConn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := mCli.RegisterNode(ctx, &godfsv1.RegisterNodeRequest{NodeId: "n1", GrpcAddress: chAddr, CapacityBytes: 1 << 30}); err != nil {
		t.Fatal(err)
	}

	cli, err := client.NewWithChunkSize(mLn.Addr().String(), chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	if err := cli.Mkdir(ctx, "/g"); err != nil {
		t.Fatal(err)
	}
	if err := cli.Create(ctx, "/g/x"); err != nil {
		t.Fatal(err)
	}

	// Start chunk server now to accept initial writes.
	go func() { _ = chSrv.Serve(chLn) }()
	defer func() { chSrv.Stop(); _ = chLn.Close() }()

	if err := cli.Write(ctx, "/g/x", []byte("gc")); err != nil {
		t.Fatal(err)
	}
	// Stop chunk server before delete to force pending deletes.
	chSrv.Stop()
	_ = chLn.Close()

	if err := cli.Delete(ctx, "/g/x"); err != nil {
		// initial delete may fail because peer is down; we still want pending deletes to be present.
		// Ignore error.
		_ = err
	}

	// Restart chunk server on the same address and run GC steps until it deletes the chunk.
	chLn, err = net.Listen("tcp", chAddr)
	if err != nil {
		t.Fatalf("re-listen chunk: %v", err)
	}
	chSrv = grpc.NewServer()
	godfsv1.RegisterChunkServiceServer(chSrv, &grpcsvc.ChunkServer{Store: st})
	go func() { _ = chSrv.Serve(chLn) }()

	gcDeadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(gcDeadline) {
		cid, addr, ok := store.PlanDeleteGC()
		if ok {
			t.Logf("gc: attempting delete chunk=%s on %s", cid, addr)
			rctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err == nil {
				_, derr := godfsv1.NewChunkServiceClient(cc).DeleteChunk(rctx, &godfsv1.DeleteChunkRequest{ChunkId: string(cid)})
				_ = cc.Close()
				if derr == nil {
					t.Logf("gc: delete ok, clearing pending")
					uctx, ucancel := context.WithTimeout(context.Background(), 2*time.Second)
					_ = store.ClearPendingDeleteAddr(uctx, cid, addr)
					ucancel()
				} else {
					t.Logf("gc: delete failed: %v", derr)
				}
			} else {
				t.Logf("gc: dial failed: %v", err)
			}
			cancel()
		} else {
			t.Logf("gc: nothing pending")
		}
		entries, _ := os.ReadDir(chDir)
		hasChk := false
		for _, e := range entries {
			if filepath.Ext(e.Name()) == ".chk" {
				hasChk = true
			}
		}
		if !hasChk {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("expected chunk files to be GCed in %s", chDir)
}

