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

func startSingleRaftMaster(t *testing.T, grpcAddr, raftAddr, raftDir string, chunkSize int64, replication int, nodeDeadAfter time.Duration) (addr string, store *raftmeta.Service) {
	t.Helper()
	node, err := raftmeta.StartNode(raftmeta.NodeConfig{
		NodeID:       "m0",
		RaftListen:   raftAddr,
		RaftDir:      raftDir,
		ChunkSize:    chunkSize,
		Replication:  replication,
		NodeDeadAfter: nodeDeadAfter,
		Peers:        nil,
		Bootstrap:    true,
	})
	if err != nil {
		t.Fatalf("start raft: %v", err)
	}
	store = raftmeta.NewService(node.Raft, node.FSM, map[string]string{raftAddr: grpcAddr})

	ln, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		t.Fatalf("listen master: %v", err)
	}
	srv := grpc.NewServer()
	godfsv1.RegisterMasterServiceServer(srv, &grpcsvc.MasterServer{Store: store})
	go func() { _ = srv.Serve(ln) }()

	// Wait until single-node raft becomes leader.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if store.IsLeader() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !store.IsLeader() {
		t.Fatalf("raft master did not become leader")
	}

	t.Cleanup(func() {
		srv.Stop()
		_ = ln.Close()
		_ = node.Close()
	})
	return ln.Addr().String(), store
}

func TestE2E_Rebalancer_HealsReplicaAfterNodeDown(t *testing.T) {
	const chunkSize = 32 * 1024
	const replication = 2
	const deadAfter = 1 * time.Second

	base := t.TempDir()
	mgrpc := "127.0.0.1:0"
	mraftLn, _ := net.Listen("tcp", "127.0.0.1:0")
	mraft := mraftLn.Addr().String()
	_ = mraftLn.Close()
	masterAddr, store := startSingleRaftMaster(t, mgrpc, mraft, filepath.Join(base, "raft"), chunkSize, replication, deadAfter)

	// Start 2 chunk servers (replication factor 2).
	chunkBase := filepath.Join(base, "chunks")
	_ = os.MkdirAll(chunkBase, 0o750)

	addChunk := func(id string) (addr string, dir string, stop func()) {
		dir = filepath.Join(chunkBase, id)
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
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := mc.RegisterNode(ctx, &godfsv1.RegisterNodeRequest{NodeId: id, GrpcAddress: ln.Addr().String(), CapacityBytes: 1 << 30}); err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
		// initial heartbeat to mark it alive
		_, _ = mc.Heartbeat(ctx, &godfsv1.HeartbeatRequest{NodeId: id, GrpcAddress: ln.Addr().String(), CapacityBytes: 1 << 30, UsedBytes: 0})
		return ln.Addr().String(), dir, func() { srv.Stop(); _ = ln.Close() }
	}

	_, dirA, stopA := addChunk("a")
	_, dirB, stopB := addChunk("b")
	defer stopA()
	defer stopB()

	// Keep heartbeating live nodes so liveness-based placement has candidates.
	hbConn, err := grpc.NewClient(masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer hbConn.Close()
	hbClient := godfsv1.NewMasterServiceClient(hbConn)
	live := map[string]bool{"a": true, "b": true}
	go func() {
		tk := time.NewTicker(200 * time.Millisecond)
		defer tk.Stop()
		for range tk.C {
			for id, ok := range live {
				if !ok {
					continue
				}
				hctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				_, _ = hbClient.Heartbeat(hctx, &godfsv1.HeartbeatRequest{NodeId: id, GrpcAddress: "", CapacityBytes: 1 << 30, UsedBytes: 0})
				cancel()
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cli, err := client.NewWithChunkSize(masterAddr, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	if err := cli.Mkdir(ctx, "/r"); err != nil {
		t.Fatal(err)
	}
	if err := cli.Create(ctx, "/r/x"); err != nil {
		t.Fatal(err)
	}
	if err := cli.Write(ctx, "/r/x", []byte("rebalance")); err != nil {
		t.Fatal(err)
	}

	// Ensure both replicas exist initially.
	for _, d := range []string{dirA, dirB} {
		entries, err := os.ReadDir(d)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, e := range entries {
			if filepath.Ext(e.Name()) == ".chk" {
				found = true
			}
		}
		if !found {
			t.Fatalf("expected .chk in %s", d)
		}
	}

	// Stop one node and add a new one.
	stopB()
	live["b"] = false
	// wait until node b is considered dead
	time.Sleep(deadAfter + 200*time.Millisecond)
	_, dirC, stopC := addChunk("c")
	defer stopC()
	live["c"] = true

	// Wait for rebalancer to copy chunk to new node.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		// Run rebalancer step manually and capture errors for debugging.
		if store.IsLeader() {
			act, err := store.PlanRebalance(time.Now().UTC())
			if err != nil {
				t.Logf("plan rebalance: %v", err)
			} else if act != nil {
				rctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if err := store.ExecuteRebalance(rctx, act); err != nil {
					t.Logf("execute rebalance: %v", err)
				}
				cancel()
			}
		}
		entries, err := os.ReadDir(dirC)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if filepath.Ext(e.Name()) == ".chk" {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("expected rebalanced .chk in %s", dirC)
}

