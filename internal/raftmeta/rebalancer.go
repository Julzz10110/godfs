package raftmeta

import (
	"context"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	godfsv1 "godfs/api/proto/godfs/v1"
	"godfs/internal/domain"
)

type RebalanceAction struct {
	ChunkID       domain.ChunkID
	SourceAddr    string
	TargetNodeID  domain.NodeID
	TargetAddr    string
}

// PlanRebalance computes a best-effort single action to heal replication.
func (s *Service) PlanRebalance(at time.Time) (*RebalanceAction, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()

	st := s.fsm.st
	if st == nil {
		return nil, fmt.Errorf("no state")
	}
	targetRF := st.ReplicationFactor
	if targetRF <= 1 {
		return nil, nil
	}

	for cid, cr := range st.Chunks {
		// count alive replicas (or all replicas if liveness disabled)
		alive := make([]domain.ChunkReplica, 0, len(cr.Replicas))
		for _, r := range cr.Replicas {
			if st.NodeDeadAfter > 0 && !st.isAliveAt(r.NodeID, at) {
				continue
			}
			alive = append(alive, r)
		}
		if len(alive) >= targetRF {
			continue
		}

		// pick target node not already in replicas and alive
		existing := map[domain.NodeID]struct{}{}
		for _, r := range cr.Replicas {
			existing[r.NodeID] = struct{}{}
		}
		var srcAddr string
		if len(alive) > 0 {
			srcAddr = alive[0].Address
		} else if len(cr.Replicas) > 0 {
			srcAddr = cr.Replicas[0].Address
		} else {
			continue
		}

		for _, n := range st.Nodes {
			if _, ok := existing[n.ID]; ok {
				continue
			}
			if st.NodeDeadAfter > 0 && !st.isAliveAt(n.ID, at) {
				continue
			}
			return &RebalanceAction{
				ChunkID:      cid,
				SourceAddr:   srcAddr,
				TargetNodeID: n.ID,
				TargetAddr:   n.GRPCAddress,
			}, nil
		}
	}
	return nil, nil
}

func (s *Service) ExecuteRebalance(ctx context.Context, act *RebalanceAction) error {
	if act == nil {
		return nil
	}
	// Ask target to PullChunk from source.
	conn, err := grpc.NewClient(act.TargetAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	ch := godfsv1.NewChunkServiceClient(conn)
	rc, err := ch.PullChunk(ctx, &godfsv1.PullChunkRequest{
		ChunkId:           string(act.ChunkID),
		SourcePeerAddress: act.SourceAddr,
	})
	if err != nil {
		return err
	}
	for {
		_, err := rc.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	// Persist the new replica in Raft.
	return s.AddReplica(ctx, act.ChunkID, act.TargetNodeID, act.TargetAddr)
}

