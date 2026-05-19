package e2e_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"

	godfsv1 "godfs/api/proto/godfs/v1"
	grpcsvc "godfs/internal/adapter/grpc"
	"godfs/internal/raftmeta"
	"godfs/pkg/client"
)

func TestE2E_RaftMembershipChange_ViaAdminRPC(t *testing.T) {
	const chunkSize = 32 * 1024
	const replication = 1

	base := t.TempDir()
	addrs := uniqueFreeAddrs(t, 8)
	grpc0, grpc1, grpc2, grpc3 := addrs[0], addrs[1], addrs[2], addrs[3]
	raft0, raft1, raft2, raft3 := addrs[4], addrs[5], addrs[6], addrs[7]

	// Bootstrap a single-node cluster.
	m0 := startRaftMaster(t, "m0", grpc0, raft0, filepath.Join(base, "m0"), fmt.Sprintf("m0@%s@%s", raft0, grpc0), chunkSize, replication, true)
	// Start other nodes (not part of configuration yet).
	m1 := startRaftMaster(t, "m1", grpc1, raft1, filepath.Join(base, "m1"), fmt.Sprintf("m0@%s@%s", raft0, grpc0), chunkSize, replication, false)
	m2 := startRaftMaster(t, "m2", grpc2, raft2, filepath.Join(base, "m2"), fmt.Sprintf("m0@%s@%s", raft0, grpc0), chunkSize, replication, false)
	m3 := startRaftMaster(t, "m3", grpc3, raft3, filepath.Join(base, "m3"), fmt.Sprintf("m0@%s@%s", raft0, grpc0), chunkSize, replication, false)

	leader := waitLeader(t, []*raftMaster{m0, m1, m2, m3}, 10*time.Second)

	// Client to leader.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cli, err := client.NewWithChunkSize(leader.grpcAddr, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	// Add m1 and m2 as voters.
	if err := cli.AddMaster(ctx, "m1", m1.raftAddr, m1.grpcAddr); err != nil {
		t.Fatalf("add master m1: %v", err)
	}
	if err := cli.AddMaster(ctx, "m2", m2.raftAddr, m2.grpcAddr); err != nil {
		t.Fatalf("add master m2: %v", err)
	}
	if err := cli.AddMaster(ctx, "m3", m3.raftAddr, m3.grpcAddr); err != nil {
		t.Fatalf("add master m3: %v", err)
	}

	masters, leaderID, err := cli.ListMasters(ctx)
	if err != nil {
		t.Fatalf("list masters: %v", err)
	}
	if leaderID == "" {
		t.Fatalf("expected leader id")
	}
	if len(masters) < 4 {
		t.Fatalf("expected >=4 masters, got %d", len(masters))
	}

	// Remove m3 (should keep >=3 voters).
	if err := cli.RemoveMaster(ctx, "m3"); err != nil {
		t.Fatalf("remove master m3: %v", err)
	}
	masters2, _, err := cli.ListMasters(ctx)
	if err != nil {
		t.Fatalf("list masters after remove: %v", err)
	}
	if len(masters2) >= len(masters) {
		t.Fatalf("expected membership to shrink, before=%d after=%d", len(masters), len(masters2))
	}

	// Sanity: leader still serves metadata writes.
	if err := cli.Mkdir(ctx, "/m"); err != nil {
		t.Fatalf("mkdir after membership change: %v", err)
	}
}

// Ensure test file links packages used in other e2e helpers.
var (
	_ = raftmeta.NodeConfig{}
	_ = grpc.NewServer
	_ = godfsv1.RegisterMasterServiceServer
	_ = grpcsvc.MasterServer{}
)
