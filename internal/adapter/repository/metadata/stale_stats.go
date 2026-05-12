package metadata

import (
	"context"
	"time"

	"google.golang.org/grpc"

	godfsv1 "godfs/api/proto/godfs/v1"
	"godfs/internal/dataplane"
	"godfs/internal/domain"
	"godfs/internal/observability"
	"godfs/internal/security"
)

// CountStaleReplicas returns the number of live chunk replicas whose on-disk checksum
// differs from the 32-byte metadata checksum. Chunks without a metadata checksum are skipped.
func (s *Store) CountStaleReplicas(ctx context.Context, at time.Time) int {
	type snap struct {
		id       domain.ChunkID
		checksum []byte
		replicas []domain.ChunkReplica
	}
	var snaps []snap
	nodeIDs := map[domain.NodeID]struct{}{}

	s.mu.RLock()
	for cid, cr := range s.chunks {
		if cr == nil || !dataplane.HasCommittedChunkChecksum(cr.checksum) {
			continue
		}
		snaps = append(snaps, snap{
			id:       cid,
			checksum: append([]byte(nil), cr.checksum...),
			replicas: append([]domain.ChunkReplica(nil), cr.replicas...),
		})
		for _, r := range cr.replicas {
			nodeIDs[r.NodeID] = struct{}{}
		}
	}
	deadAfter := s.nodeDeadAfter
	alive := make(map[domain.NodeID]bool, len(nodeIDs))
	for id := range nodeIDs {
		alive[id] = s.isAliveAt(id, at)
	}
	s.mu.RUnlock()

	repSum := repChecksumFuncMeta(s)
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
		total += countStaleReplicasOneChunkMeta(ctx, p.id, p.checksum, p.replicas, isAlive, repSum)
	}
	return total
}

func countStaleReplicasOneChunkMeta(
	ctx context.Context,
	chunkID domain.ChunkID,
	metaSum []byte,
	replicas []domain.ChunkReplica,
	isAlive func(domain.NodeID) bool,
	repSum func(context.Context, string, domain.ChunkID) ([]byte, error),
) int {
	if !dataplane.HasCommittedChunkChecksum(metaSum) {
		return 0
	}
	n := 0
	for _, r := range replicas {
		if !isAlive(r.NodeID) {
			continue
		}
		sum, err := repSum(ctx, r.Address, chunkID)
		if err != nil {
			observability.RecordMaintReplicaMetaCompare("rpc_error")
			continue
		}
		if !dataplane.HasCommittedChunkChecksum(sum) {
			observability.RecordMaintReplicaMetaCompare("short_checksum")
			continue
		}
		if dataplane.IsReplicaStaleComparedToMeta(metaSum, sum) {
			observability.RecordMaintReplicaMetaCompare("mismatch")
			n++
		} else {
			observability.RecordMaintReplicaMetaCompare("match")
		}
	}
	return n
}

func repChecksumFuncMeta(store *Store) func(context.Context, string, domain.ChunkID) ([]byte, error) {
	return func(ctx context.Context, addr string, chunkID domain.ChunkID) ([]byte, error) {
		store.mu.RLock()
		v := store.checksumVerifier
		store.mu.RUnlock()
		if v != nil {
			return v(ctx, addr, chunkID)
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
