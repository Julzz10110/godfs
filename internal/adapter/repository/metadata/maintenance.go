package metadata

import (
	"bytes"
	"context"
	"io"
	"time"

	"google.golang.org/grpc"

	godfsv1 "godfs/api/proto/godfs/v1"
	"godfs/internal/domain"
	"godfs/internal/security"
)

// RebalanceAction describes one rebalance or stale-replica repair step on the in-memory metadata store.
type RebalanceAction struct {
	ChunkID        domain.ChunkID
	SourceAddr     string
	TargetNodeID   domain.NodeID
	TargetAddr     string
	RepairExisting bool
}

type chunkPlan struct {
	id       domain.ChunkID
	checksum []byte
	replicas []domain.ChunkReplica
}

func (s *Store) rebalanceDueLocked(cid domain.ChunkID, at time.Time) bool {
	t := s.rebalanceTasks[cid]
	if t == nil {
		return true
	}
	if t.NextAttemptUnix == 0 {
		return true
	}
	return !time.Unix(t.NextAttemptUnix, 0).After(at)
}

// PlanRebalance picks one stale-replica repair or one under-replication heal. Intended to be called only from the master process.
func (s *Store) PlanRebalance(at time.Time) (*RebalanceAction, error) {
	s.mu.RLock()
	targetRF := s.replicationFactor
	deadAfter := s.nodeDeadAfter
	var plans []chunkPlan
	for cid, cr := range s.chunks {
		if !s.rebalanceDueLocked(cid, at) {
			continue
		}
		plans = append(plans, chunkPlan{
			id:       cid,
			checksum: append([]byte(nil), cr.checksum...),
			replicas: append([]domain.ChunkReplica(nil), cr.replicas...),
		})
	}
	nodes := append([]domain.ChunkNode(nil), s.nodes...)
	aliveAt := make(map[domain.NodeID]bool)
	for _, n := range s.nodes {
		if deadAfter <= 0 {
			aliveAt[n.ID] = true
		} else {
			aliveAt[n.ID] = s.isAliveAt(n.ID, at)
		}
	}
	s.mu.RUnlock()
	isAlive := func(id domain.NodeID) bool {
		if deadAfter <= 0 {
			return true
		}
		return aliveAt[id]
	}

	if targetRF <= 1 {
		return nil, nil
	}

	repChecksum := func(addr string, chunkID domain.ChunkID) ([]byte, error) {
		dopts, err := security.ClientDialOptions()
		if err != nil {
			return nil, err
		}
		cc, err := grpc.NewClient(addr, dopts...)
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

	for _, p := range plans {
		if len(p.checksum) != 32 || len(p.replicas) == 0 {
			continue
		}
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

// ExecuteRebalance runs PullChunk on the target (repair or new replica).
func (s *Store) ExecuteRebalance(ctx context.Context, act *RebalanceAction) error {
	if act == nil {
		return nil
	}
	dopts, err := security.ClientDialOptions()
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient(act.TargetAddr, dopts...)
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
	if act.RepairExisting {
		return nil
	}
	return s.AddReplica(act.ChunkID, act.TargetNodeID, act.TargetAddr)
}

// AddReplica appends a replica placement after a successful PullChunk to a new node.
func (s *Store) AddReplica(chunkID domain.ChunkID, nodeID domain.NodeID, addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cr, ok := s.chunks[chunkID]
	if !ok {
		return domain.ErrNotFound
	}
	for _, r := range cr.replicas {
		if r.NodeID == nodeID {
			return nil
		}
	}
	if _, ok := s.nodeSet[nodeID]; !ok {
		return domain.ErrNotFound
	}
	s.nodeUsedBytes[nodeID] += s.chunkSize
	cr.replicas = append(cr.replicas, domain.ChunkReplica{NodeID: nodeID, Address: addr})
	return nil
}

// PlanDeleteGC returns one pending chunk delete on a peer that is due.
func (s *Store) PlanDeleteGC(at time.Time) (chunkID domain.ChunkID, addr string, attempts int, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for cid, addrs := range s.pendingDeletes {
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

// ClearPendingDeleteAddr removes a peer address from the pending delete set after success.
func (s *Store) ClearPendingDeleteAddr(chunkID domain.ChunkID, addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	set := s.pendingDeletes[chunkID]
	if set == nil {
		return
	}
	delete(set, addr)
	if len(set) == 0 {
		delete(s.pendingDeletes, chunkID)
	}
}

// MarkPendingDeleteAttempt records backoff for a failed DeleteChunk RPC.
func (s *Store) MarkPendingDeleteAttempt(chunkID domain.ChunkID, addr string, attempts int, nextAttemptUnix int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	set := s.pendingDeletes[chunkID]
	if set == nil {
		return
	}
	p := set[addr]
	if p == nil {
		return
	}
	p.Attempts = attempts
	p.NextAttemptUnix = nextAttemptUnix
}

// MarkRebalanceAttempt records backoff for a failed rebalance.
func (s *Store) MarkRebalanceAttempt(chunkID domain.ChunkID, attempts int, nextAttemptUnix int64, lastErr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.rebalanceTasks[chunkID]
	if t == nil {
		t = &rebalanceWork{}
		s.rebalanceTasks[chunkID] = t
	}
	t.Attempts = attempts
	t.NextAttemptUnix = nextAttemptUnix
	t.LastError = lastErr
}

// ClearRebalanceTask removes backoff state for a chunk after success.
func (s *Store) ClearRebalanceTask(chunkID domain.ChunkID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.rebalanceTasks, chunkID)
}

// RebalanceAttempts returns current attempt count for backoff decisions.
func (s *Store) RebalanceAttempts(chunkID domain.ChunkID) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if t := s.rebalanceTasks[chunkID]; t != nil {
		return t.Attempts
	}
	return 0
}

// SnapshotChunkIDs returns all chunk ids known to metadata.
func (s *Store) SnapshotChunkIDs() map[domain.ChunkID]struct{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[domain.ChunkID]struct{}, len(s.chunks))
	for cid := range s.chunks {
		out[cid] = struct{}{}
	}
	return out
}

// SnapshotNodes returns registered chunk nodes.
func (s *Store) SnapshotNodes() []domain.ChunkNode {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]domain.ChunkNode(nil), s.nodes...)
}

// OrphanGCOnce deletes chunk files on nodes that are not referenced by metadata (best-effort).
func (s *Store) OrphanGCOnce(ctx context.Context, minAge time.Duration, maxDeletesPerNode int) error {
	known := s.SnapshotChunkIDs()
	nodes := s.SnapshotNodes()
	now := time.Now().UTC()

	for _, n := range nodes {
		s.mu.RLock()
		deadAfter := s.nodeDeadAfter
		alive := true
		if deadAfter > 0 {
			alive = s.isAliveAt(n.ID, now)
		}
		s.mu.RUnlock()
		if !alive {
			continue
		}
		dopts, err := security.ClientDialOptions()
		if err != nil {
			continue
		}
		cc, err := grpc.NewClient(n.GRPCAddress, dopts...)
		if err != nil {
			continue
		}
		cli := godfsv1.NewChunkServiceClient(cc)
		lr, err := cli.ListChunks(ctx, &godfsv1.ListChunksRequest{})
		_ = cc.Close()
		if err != nil {
			continue
		}
		deleted := 0
		for _, info := range lr.Chunks {
			id := info.ChunkId
			cid := domain.ChunkID(id)
			if _, ok := known[cid]; ok {
				continue
			}
			if minAge > 0 {
				mt := time.Unix(info.ModifiedAtUnix, 0).UTC()
				if now.Sub(mt) < minAge {
					continue
				}
			}
			if maxDeletesPerNode > 0 && deleted >= maxDeletesPerNode {
				break
			}
			cc2, err := grpc.NewClient(n.GRPCAddress, dopts...)
			if err != nil {
				continue
			}
			_, _ = godfsv1.NewChunkServiceClient(cc2).DeleteChunk(ctx, &godfsv1.DeleteChunkRequest{ChunkId: id})
			_ = cc2.Close()
			deleted++
		}
	}
	return nil
}
