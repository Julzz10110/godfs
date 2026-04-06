package raftmeta

import (
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"godfs/internal/config"
)

type NodeConfig struct {
	NodeID      string
	RaftListen  string
	RaftDir     string
	GRPCAdvertise string // optional: used for redirects (should match peers list)

	ChunkSize    int64
	Replication  int
	LeaseDur     time.Duration

	Peers        map[string]raft.ServerAddress
	Bootstrap    bool
}

type Node struct {
	Raft *raft.Raft
	FSM  *FSM

	transport *raft.NetworkTransport
	logStore  *raftboltdb.BoltStore
	stableStore *raftboltdb.BoltStore
}

func StartNode(cfg NodeConfig) (*Node, error) {
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("NodeID required")
	}
	if cfg.RaftListen == "" {
		return nil, fmt.Errorf("RaftListen required")
	}
	if cfg.RaftDir == "" {
		return nil, fmt.Errorf("RaftDir required")
	}
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = config.DefaultChunkSize
	}
	if cfg.Replication <= 0 {
		cfg.Replication = config.DefaultReplication
	}
	if cfg.LeaseDur <= 0 {
		cfg.LeaseDur = time.Duration(config.DefaultLeaseSec) * time.Second
	}
	if err := os.MkdirAll(cfg.RaftDir, 0o750); err != nil {
		return nil, err
	}

	rconf := raft.DefaultConfig()
	rconf.LocalID = raft.ServerID(cfg.NodeID)

	addr, err := net.ResolveTCPAddr("tcp", cfg.RaftListen)
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(cfg.RaftListen, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.RaftDir, "raft-log.db"))
	if err != nil {
		return nil, err
	}
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.RaftDir, "raft-stable.db"))
	if err != nil {
		return nil, err
	}
	snapStore, err := raft.NewFileSnapshotStore(cfg.RaftDir, 2, os.Stderr)
	if err != nil {
		return nil, err
	}

	fsm := NewFSM(NewState(cfg.ChunkSize, cfg.Replication, cfg.LeaseDur))
	r, err := raft.NewRaft(rconf, fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		return nil, err
	}

	hasState, err := raft.HasExistingState(logStore, stableStore, snapStore)
	if err != nil {
		return nil, err
	}
	if cfg.Bootstrap && !hasState {
		// Ensure deterministic ordering of initial configuration across nodes.
		peerMap := make(map[string]raft.ServerAddress, len(cfg.Peers)+1)
		for k, v := range cfg.Peers {
			peerMap[k] = v
		}
		if _, ok := peerMap[cfg.NodeID]; !ok {
			peerMap[cfg.NodeID] = raft.ServerAddress(cfg.RaftListen)
		}
		ids := make([]string, 0, len(peerMap))
		for id := range peerMap {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		servers := make([]raft.Server, 0, len(ids))
		for _, id := range ids {
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(id),
				Address: peerMap[id],
			})
		}
		f := r.BootstrapCluster(raft.Configuration{Servers: servers})
		if err := f.Error(); err != nil {
			return nil, err
		}
		log.Printf("raft bootstrap complete (node=%s addr=%s)", cfg.NodeID, cfg.RaftListen)
	}

	return &Node{Raft: r, FSM: fsm, transport: transport, logStore: logStore, stableStore: stableStore}, nil
}

func (n *Node) Close() error {
	if n == nil {
		return nil
	}
	if n.Raft != nil {
		_ = n.Raft.Shutdown().Error()
	}
	if n.transport != nil {
		_ = n.transport.Close()
	}
	if n.logStore != nil {
		_ = n.logStore.Close()
	}
	if n.stableStore != nil {
		_ = n.stableStore.Close()
	}
	return nil
}

