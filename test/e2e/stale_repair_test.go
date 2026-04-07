package e2e_test

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
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

func TestE2E_StaleReplica_RepairedByRebalancer(t *testing.T) {
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

	// Start 2 chunk servers.
	type chunkNode struct {
		id   string
		addr string
		dir  string
		srv  *grpc.Server
		ln   net.Listener
	}
	startChunk := func(id string) *chunkNode {
		dir := filepath.Join(base, "chunk-"+id)
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

		// register node
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

		return &chunkNode{id: id, addr: ln.Addr().String(), dir: dir, srv: srv, ln: ln}
	}

	n1 := startChunk("n1")
	defer func() { n1.srv.Stop(); _ = n1.ln.Close() }()
	n2 := startChunk("n2")
	defer func() { n2.srv.Stop(); _ = n2.ln.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cli, err := client.NewWithChunkSize(mLn.Addr().String(), chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	if err := cli.Mkdir(ctx, "/s"); err != nil {
		t.Fatal(err)
	}
	if err := cli.Create(ctx, "/s/x"); err != nil {
		t.Fatal(err)
	}
	payload := []byte("stale-repair")
	if err := cli.Write(ctx, "/s/x", payload); err != nil {
		t.Fatal(err)
	}

	// Determine chunk id and replicas.
	mConn, err := grpc.NewClient(mLn.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer mConn.Close()
	mc := godfsv1.NewMasterServiceClient(mConn)
	gr, err := mc.GetChunkForRead(ctx, &godfsv1.GetChunkForReadRequest{Path: "/s/x", Offset: 0})
	if err != nil {
		t.Fatal(err)
	}
	chunkID := gr.ChunkId

	// Corrupt the chunk file on one replica (pick n2).
	entries, err := os.ReadDir(n2.dir)
	if err != nil {
		t.Fatal(err)
	}
	var chkName string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".chk") {
			chkName = e.Name()
		}
	}
	if chkName == "" {
		t.Fatalf("no .chk in %s", n2.dir)
	}
	if !strings.HasPrefix(chkName, chunkID) && !strings.Contains(chkName, chunkID) {
		// filename is chunkID.chk; still proceed
	}
	if err := os.WriteFile(filepath.Join(n2.dir, chunkID+".chk"), []byte("CORRUPTED"), 0o640); err != nil {
		t.Fatal(err)
	}

	// Read should still succeed (client checksum verify will fall back).
	got, err := cli.Read(ctx, "/s/x")
	if err != nil {
		t.Fatalf("read after corruption: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("got %q want %q", got, payload)
	}

	// Run rebalancer until replica is repaired (checksum matches master checksum).
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		act, err := store.PlanRebalance(time.Now().UTC())
		if err == nil && act != nil {
			rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = store.ExecuteRebalance(rctx, act)
			rcancel()
			uctx, ucancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = store.ClearRebalanceTask(uctx, act.ChunkID)
			ucancel()
		}
		// Check checksum on n2 equals master checksum from GetChunkForRead.
		cc, err := grpc.NewClient(n2.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			resp, err2 := godfsv1.NewChunkServiceClient(cc).ChecksumChunk(ctx, &godfsv1.ChecksumChunkRequest{ChunkId: chunkID})
			_ = cc.Close()
			if err2 == nil && len(resp.ChecksumSha256) == 32 && len(gr.ChunkChecksumSha256) == 32 && string(resp.ChecksumSha256) == string(gr.ChunkChecksumSha256) {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("stale replica was not repaired in time")
}

