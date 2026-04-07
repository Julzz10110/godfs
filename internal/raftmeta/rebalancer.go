package raftmeta

import (
	"bytes"
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
	// RepairExisting indicates the target is an existing replica to be overwritten (stale repair).
	RepairExisting bool
}

type chunkPlan struct {
	id       domain.ChunkID
	checksum []byte
	replicas []domain.ChunkReplica
}

// PlanRebalance computes a single best-effort action:
// - Prefer repairing stale replicas (checksum mismatch vs metadata checksum) when possible.
// - Otherwise, heal under-replication by adding a missing replica.
func (s *Service) PlanRebalance(at time.Time) (*RebalanceAction, error) {
	// Snapshot state under lock; do network calls outside.
	s.fsm.mu.RLock()
	st := s.fsm.st
	if st == nil {
		s.fsm.mu.RUnlock()
		return nil, fmt.Errorf("no state")
	}
	targetRF := st.ReplicationFactor
	deadAfter := st.NodeDeadAfter
	var plans []chunkPlan
	for cid, cr := range st.Chunks {
		if !st.RebalanceDue(cid, at) {
			continue
		}
		cp := chunkPlan{
			id:       cid,
			checksum: append([]byte(nil), cr.Checksum...),
			replicas: append([]domain.ChunkReplica(nil), cr.Replicas...),
		}
		plans = append(plans, cp)
	}
	nodes := append([]domain.ChunkNode(nil), st.Nodes...)
	isAlive := func(id domain.NodeID) bool {
		if deadAfter <= 0 {
			return true
		}
		return st.isAliveAt(id, at)
	}
	s.fsm.mu.RUnlock()

	if targetRF <= 1 {
		return nil, nil
	}

	// Helper: compute checksum from a replica.
	repChecksum := func(addr string, chunkID domain.ChunkID) ([]byte, error) {
		cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		defer cc.Close()
		cli := godfsv1.NewChunkServiceClient(cc)
		resp, err := cli.ChecksumChunk(context.Background(), &godfsv1.ChecksumChunkRequest{ChunkId: string(chunkID)})
		if err != nil {
			return nil, err
		}
		return resp.ChecksumSha256, nil
	}

	// 1) Stale repair: if master checksum exists and we can find a good source.
	for _, p := range plans {
		if len(p.checksum) != 32 || len(p.replicas) == 0 {
			continue
		}
		// Find a good source matching metadata checksum.
		var goodSrc string
		for _, r := range p.replicas {
			if !isAlive(r.NodeID) {
				continue
			}
			sum, err := repChecksum(r.Address, p.id)
			if err == nil && len(sum) == 32 && bytes.Equal(sum, p.checksum) {
				goodSrc = r.Address
				break
			}
		}
		if goodSrc == "" {
			continue
		}
		// Find a stale replica to repair.
		for _, r := range p.replicas {
			if !isAlive(r.NodeID) {
				continue
			}
			sum, err := repChecksum(r.Address, p.id)
			if err != nil || len(sum) != 32 {
				continue
			}
			if !bytes.Equal(sum, p.checksum) {
				return &RebalanceAction{
					ChunkID:        p.id,
					SourceAddr:     goodSrc,
					TargetNodeID:   r.NodeID,
					TargetAddr:     r.Address,
					RepairExisting: true,
				}, nil
			}
		}
	}

	// 2) Under-replication: add a new replica.
	for _, p := range plans {
		alive := make([]domain.ChunkReplica, 0, len(p.replicas))
		for _, r := range p.replicas {
			if !isAlive(r.NodeID) {
				continue
			}
			alive = append(alive, r)
		}
		if len(alive) >= targetRF {
			continue
		}
		existing := map[domain.NodeID]struct{}{}
		for _, r := range p.replicas {
			existing[r.NodeID] = struct{}{}
		}
		srcAddr := ""
		if len(alive) > 0 {
			srcAddr = alive[0].Address
		} else if len(p.replicas) > 0 {
			srcAddr = p.replicas[0].Address
		}
		if srcAddr == "" {
			continue
		}
		for _, n := range nodes {
			if _, ok := existing[n.ID]; ok {
				continue
			}
			if !isAlive(n.ID) {
				continue
			}
			return &RebalanceAction{
				ChunkID:      p.id,
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
	if act.RepairExisting {
		// no metadata change
		return nil
	}
	return s.AddReplica(ctx, act.ChunkID, act.TargetNodeID, act.TargetAddr)
}

// PlanDeleteGC returns at most one due pending delete action.
func (s *Service) PlanDeleteGC(at time.Time) (chunkID domain.ChunkID, addr string, attempts int, ok bool) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	for cid, addrs := range s.fsm.st.PendingDeletes {
		for a, pd := range addrs {
			if pd == nil {
				continue
			}
			if pd.NextAttemptUnix > 0 && time.Unix(pd.NextAttemptUnix, 0).After(at) {
				continue
			}
			return cid, a, pd.Attempts, true
		}
	}
	return "", "", 0, false
}


