package e2e_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	godfsv1 "godfs/api/proto/godfs/v1"
	grpcsvc "godfs/internal/adapter/grpc"
	"godfs/internal/raftmeta"
	"godfs/pkg/client"
)

type raftMaster struct {
	nodeID   string
	grpcAddr string
	raftAddr string
	raftDir  string
	grpcLn   net.Listener
	grpcSrv  *grpc.Server
	raftNode *raftmeta.Node
	store    *raftmeta.Service
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// uniqueFreeAddrs returns n distinct host:port strings. Sequential freeAddr calls
// can otherwise reuse a recently closed ephemeral port, colliding with an
// earlier address once listeners are bound (e.g. raft port == another node's gRPC).
func uniqueFreeAddrs(t *testing.T, n int) []string {
	t.Helper()
	if n <= 0 {
		return nil
	}
	out := make([]string, 0, n)
	seen := make(map[string]struct{}, n)
	for len(out) < n {
		a := freeAddr(t)
		if _, ok := seen[a]; ok {
			continue
		}
		seen[a] = struct{}{}
		out = append(out, a)
	}
	return out
}

func startRaftMaster(t *testing.T, nodeID, grpcAddr, raftAddr, raftDir, peersRaw string, chunkSize int64, replication int, bootstrap bool) *raftMaster {
	t.Helper()

	peers, grpcByRaft, err := raftmeta.ParsePeers(peersRaw)
	if err != nil {
		t.Fatalf("parse peers: %v", err)
	}
	node, err := raftmeta.StartNode(raftmeta.NodeConfig{
		NodeID:      nodeID,
		RaftListen:  raftAddr,
		RaftDir:     raftDir,
		ChunkSize:   chunkSize,
		Replication: replication,
		Peers:       peers,
		Bootstrap:   bootstrap,
	})
	if err != nil {
		t.Fatalf("start raft node: %v", err)
	}
	store := raftmeta.NewService(node.Raft, node.FSM, grpcByRaft)

	ln, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		t.Fatalf("listen grpc: %v", err)
	}
	srv := grpc.NewServer()
	godfsv1.RegisterMasterServiceServer(srv, &grpcsvc.MasterServer{Store: store})
	go func() { _ = srv.Serve(ln) }()

	m := &raftMaster{
		nodeID:   nodeID,
		grpcAddr: ln.Addr().String(),
		raftAddr: raftAddr,
		raftDir:  raftDir,
		grpcLn:   ln,
		grpcSrv:  srv,
		raftNode: node,
		store:    store,
	}
	t.Cleanup(func() {
		srv.Stop()
		_ = ln.Close()
		_ = node.Close()
	})
	return m
}

func waitLeader(t *testing.T, masters []*raftMaster, timeout time.Duration) *raftMaster {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, m := range masters {
			if m.store.IsLeader() {
				return m
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("no leader elected within %s", timeout)
	return nil
}

func stopMaster(t *testing.T, m *raftMaster) {
	t.Helper()
	m.grpcSrv.Stop()
	_ = m.grpcLn.Close()
	_ = m.raftNode.Close()
}

func TestE2E_RaftMaster_ReplicationAndFailover(t *testing.T) {
	const chunkSize = 32 * 1024
	const replication = 1 // keep focus on Raft control plane

	base := t.TempDir()

	addrs := uniqueFreeAddrs(t, 6)
	grpc0, grpc1, grpc2 := addrs[0], addrs[1], addrs[2]
	raft0, raft1, raft2 := addrs[3], addrs[4], addrs[5]

	peersFull := fmt.Sprintf(
		"m0@%s@%s,m1@%s@%s,m2@%s@%s",
		raft0, grpc0,
		raft1, grpc1,
		raft2, grpc2,
	)

	// Bootstrap a single-node cluster; then join other masters as voters.
	m0 := startRaftMaster(t, "m0", grpc0, raft0, filepath.Join(base, "m0"), fmt.Sprintf("m0@%s@%s", raft0, grpc0), chunkSize, replication, true)
	m1 := startRaftMaster(t, "m1", grpc1, raft1, filepath.Join(base, "m1"), peersFull, chunkSize, replication, false)
	m2 := startRaftMaster(t, "m2", grpc2, raft2, filepath.Join(base, "m2"), peersFull, chunkSize, replication, false)

	masters := []*raftMaster{m0, m1, m2}
	leader := waitLeader(t, masters, 10*time.Second)

	// Ensure all nodes are members of the cluster.
	for _, m := range masters {
		if m == leader {
			continue
		}
		f := leader.raftNode.Raft.AddVoter(raft.ServerID(m.nodeID), raft.ServerAddress(m.raftAddr), 0, 5*time.Second)
		if err := f.Error(); err != nil {
			t.Fatalf("add voter %s: %v", m.nodeID, err)
		}
	}
	time.Sleep(300 * time.Millisecond)

	// Basic metadata write should work on leader and replicate.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cli, err := client.NewWithChunkSize(leader.grpcAddr, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	if err := cli.Mkdir(ctx, "/a"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := cli.Create(ctx, "/a/x"); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Heartbeat should be accepted by the leader (and replicated to followers via Raft state).
	hctx, hcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer hcancel()
	connHB, err := grpc.NewClient(leader.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial leader for heartbeat: %v", err)
	}
	defer connHB.Close()
	mc := godfsv1.NewMasterServiceClient(connHB)
	if _, err := mc.Heartbeat(hctx, &godfsv1.HeartbeatRequest{
		NodeId:        "chunk-1",
		GrpcAddress:   "127.0.0.1:8000",
		CapacityBytes: 100,
		UsedBytes:     12,
	}); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	// Ensure followers have the new file metadata (eventual apply).
	for _, m := range masters {
		c2, err := client.NewWithChunkSize(m.grpcAddr, chunkSize)
		if err != nil {
			t.Fatal(err)
		}
		ok := false
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			_, err = c2.Stat(ctx, "/a/x")
			if err == nil {
				ok = true
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		_ = c2.Close()
		if !ok {
			t.Fatalf("stat on %s did not replicate in time: last=%v", m.nodeID, err)
		}
	}

	// Concurrent metadata mutations on the leader while the cluster is stable.
	const loadWorkers = 8
	var loadWG sync.WaitGroup
	loadErrs := make(chan error, loadWorkers)
	for w := 0; w < loadWorkers; w++ {
		loadWG.Add(1)
		go func(w int) {
			defer loadWG.Done()
			cw, err := client.NewWithChunkSize(leader.grpcAddr, chunkSize)
			if err != nil {
				loadErrs <- err
				return
			}
			defer cw.Close()
			root := fmt.Sprintf("/ld%d", w)
			if err := cw.Mkdir(ctx, root); err != nil {
				loadErrs <- err
				return
			}
			for i := 0; i < 25; i++ {
				sub := fmt.Sprintf("%s/n%d", root, i)
				if err := cw.Mkdir(ctx, sub); err != nil {
					loadErrs <- err
					return
				}
			}
		}(w)
	}
	loadWG.Wait()
	close(loadErrs)
	for err := range loadErrs {
		if err != nil {
			t.Fatalf("concurrent load: %v", err)
		}
	}

	// Kill leader, then ensure a new leader can accept writes.
	stopMaster(t, leader)
	var remaining []*raftMaster
	for _, m := range masters {
		if m != leader {
			remaining = append(remaining, m)
		}
	}
	newLeader := waitLeader(t, remaining, 10*time.Second)

	cli2, err := client.NewWithChunkSize(newLeader.grpcAddr, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	defer cli2.Close()
	if err := cli2.Mkdir(ctx, "/b"); err != nil {
		t.Fatalf("mkdir after failover: %v", err)
	}
	if err := cli2.Create(ctx, "/b/y"); err != nil {
		t.Fatalf("create after failover: %v", err)
	}
	if _, err := os.Stat(base); err != nil { // keep compiler from pruning base usage
		t.Fatal(err)
	}
}
