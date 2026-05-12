package raftmeta

import (
	"bytes"
	"context"
	"time"

	"google.golang.org/grpc"

	godfsv1 "godfs/api/proto/godfs/v1"
	"godfs/internal/domain"
	"godfs/internal/security"
)

// CountStaleReplicas returns the number of live chunk replicas whose on-disk checksum
// differs from the 32-byte metadata checksum. Chunks without a metadata checksum are skipped.
// Uses the configured ChecksumVerifier when set; otherwise issues ChecksumChunk RPCs.
// Best-effort: RPC errors on a replica are not counted as stale (same heuristic as PlanRebalance).
func (s *Service) CountStaleReplicas(ctx context.Context, at time.Time) int {
	s.fsm.mu.RLock()
	st := s.fsm.st
	if st == nil {
		s.fsm.mu.RUnlock()
		return 0
	}
	type snap struct {
		id       domain.ChunkID
		checksum []byte
		replicas []domain.ChunkReplica
	}
	var snaps []snap
	nodeIDs := map[domain.NodeID]struct{}{}
	for cid, cr := range st.Chunks {
		if cr == nil || len(cr.Checksum) != 32 {
			continue
		}
		snaps = append(snaps, snap{
			id:       cid,
			checksum: append([]byte(nil), cr.Checksum...),
			replicas: append([]domain.ChunkReplica(nil), cr.Replicas...),
		})
		for _, r := range cr.Replicas {
			nodeIDs[r.NodeID] = struct{}{}
		}
	}
	deadAfter := st.NodeDeadAfter
	alive := make(map[domain.NodeID]bool, len(nodeIDs))
	for id := range nodeIDs {
		alive[id] = st.isAliveAt(id, at)
	}
	verifier := s.checksumVerifier
	s.fsm.mu.RUnlock()

	repSum := repChecksumFunc(verifier)
	isAlive := func(id domain.NodeID) bool {
		if deadAfter <= 0 {
			return true
		}
		a, ok := alive[id]
		if !ok {
			return true
		}
		return a
	}

	total := 0
	for _, p := range snaps {
		total += countStaleReplicasOneChunk(ctx, p.id, p.checksum, p.replicas, isAlive, repSum)
	}
	return total
}

func countStaleReplicasOneChunk(
	ctx context.Context,
	chunkID domain.ChunkID,
	metaSum []byte,
	replicas []domain.ChunkReplica,
	isAlive func(domain.NodeID) bool,
	repSum func(context.Context, string, domain.ChunkID) ([]byte, error),
) int {
	if len(metaSum) != 32 {
		return 0
	}
	n := 0
	for _, r := range replicas {
		if !isAlive(r.NodeID) {
			continue
		}
		sum, err := repSum(ctx, r.Address, chunkID)
		if err != nil || len(sum) != 32 {
			continue
		}
		if !bytes.Equal(sum, metaSum) {
			n++
		}
	}
	return n
}

func repChecksumFunc(verifier ChecksumVerifier) func(context.Context, string, domain.ChunkID) ([]byte, error) {
	return func(ctx context.Context, addr string, chunkID domain.ChunkID) ([]byte, error) {
		if verifier != nil {
			return verifier(ctx, addr, chunkID)
		}
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
		resp, err := cli.ChecksumChunk(ctx, &godfsv1.ChecksumChunkRequest{ChunkId: string(chunkID)})
		if err != nil {
			return nil, err
		}
		return resp.ChecksumSha256, nil
	}
}
