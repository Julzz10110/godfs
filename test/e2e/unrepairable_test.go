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

func TestE2E_Rebalancer_MarksUnrepairableWhenNoGoodReplica(t *testing.T) {
	const chunkSize = 32 * 1024
	const replication = 2

	base := t.TempDir()

	// Single-node raft master.
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

	startChunk := func(id string) (addr, dir string, srv *grpc.Server, ln net.Listener) {
		dir = filepath.Join(base, "chunk-"+id)
		_ = os.MkdirAll(dir, 0o750)
		st, err := chstor.NewFSStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		ln, err = net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		srv = grpc.NewServer()
		godfsv1.RegisterChunkServiceServer(srv, &grpcsvc.ChunkServer{Store: st})
		go func() { _ = srv.Serve(ln) }()

		mConn, err := grpc.NewClient(mLn.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		mc := godfsv1.NewMasterServiceClient(mConn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err = mc.RegisterNode(ctx, &godfsv1.RegisterNodeRequest{NodeId: id, GrpcAddress: ln.Addr().String(), CapacityBytes: 1 << 30})
		cancel()
		_ = mConn.Close()
		if err != nil {
			t.Fatalf("register %s: %v", id, err)
		}

		return ln.Addr().String(), dir, srv, ln
	}

	_, dir1, srv1, ln1 := startChunk("n1")
	defer func() { srv1.Stop(); _ = ln1.Close() }()
	_, dir2, srv2, ln2 := startChunk("n2")
	defer func() { srv2.Stop(); _ = ln2.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cli, err := client.NewWithChunkSize(mLn.Addr().String(), chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	if err := cli.Mkdir(ctx, "/u"); err != nil {
		t.Fatal(err)
	}
	if err := cli.Create(ctx, "/u/x"); err != nil {
		t.Fatal(err)
	}
	payload := []byte("unrepairable")
	if err := cli.Write(ctx, "/u/x", payload); err != nil {
		t.Fatal(err)
	}

	// Determine chunk id.
	mConn, err := grpc.NewClient(mLn.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mConn.Close() }()
	mc := godfsv1.NewMasterServiceClient(mConn)
	gr, err := mc.GetChunkForRead(ctx, &godfsv1.GetChunkForReadRequest{Path: "/u/x", Offset: 0})
	if err != nil {
		t.Fatal(err)
	}
	chunkID := gr.ChunkId

	// Corrupt all replicas so there is no good source for repair.
	if err := os.WriteFile(filepath.Join(dir1, chunkID+".chk"), []byte("BAD1"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir2, chunkID+".chk"), []byte("BAD2"), 0o640); err != nil {
		t.Fatal(err)
	}

	act, err := store.PlanRebalance(time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if act == nil || !act.Unrepairable {
		t.Fatalf("expected unrepairable action, got %+v", act)
	}

	uctx, ucancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = store.MarkRebalanceAttempt(uctx, act.ChunkID, 1, time.Now().Add(30*time.Second).Unix(), "unrepairable:"+act.UnrepairableReason)
	ucancel()

	st := store.DataPlaneStats(time.Now().UTC())
	if st.UnrepairableChunks < 1 {
		t.Fatalf("expected UnrepairableChunks>=1, got %d", st.UnrepairableChunks)
	}
}

