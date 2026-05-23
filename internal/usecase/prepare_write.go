package usecase

import (
	"context"
	"fmt"

	"godfs/internal/domain"
)

// PrepareWrite validates inputs and reserves a chunk write on the metadata store.
func PrepareWrite(ctx context.Context, store MasterStore, path string, offset, length int64) (
	chunkID domain.ChunkID,
	primaryAddr string,
	secondaryAddrs []string,
	primaryNodeID domain.NodeID,
	leaseID domain.LeaseID,
	chunkIndex int64,
	chunkOff int64,
	chunkSize int64,
	version uint64,
	err error,
) {
	p, err := NormalizeFSPath(path)
	if err != nil {
		return "", "", nil, "", "", 0, 0, 0, 0, err
	}
	if length <= 0 {
		return "", "", nil, "", "", 0, 0, 0, 0, fmt.Errorf("invalid length")
	}
	return store.PrepareWrite(ctx, p, offset, length)
}
