package usecase

import (
	"context"
	"fmt"

	"godfs/internal/domain"
)

// ChunkReadPlan describes how to read a span from a file chunk.
type ChunkReadPlan struct {
	ChunkID      domain.ChunkID
	ReplicaLocs  []domain.ChunkReplica
	ChunkOffset  int64
	Available    int64
	Version      uint64
	Checksum     []byte
}

// GetChunkForRead resolves chunk placement for a file offset.
func GetChunkForRead(ctx context.Context, store MasterStore, path string, offset int64) (ChunkReadPlan, error) {
	p, err := NormalizeFSPath(path)
	if err != nil {
		return ChunkReadPlan{}, err
	}
	if offset < 0 {
		return ChunkReadPlan{}, fmt.Errorf("invalid offset")
	}
	cid, locs, off, avail, ver, sum, err := store.GetChunkForRead(ctx, p, offset)
	if err != nil {
		return ChunkReadPlan{}, err
	}
	return ChunkReadPlan{
		ChunkID:     cid,
		ReplicaLocs: locs,
		ChunkOffset: off,
		Available:   avail,
		Version:     ver,
		Checksum:    sum,
	}, nil
}
