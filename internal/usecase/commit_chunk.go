package usecase

import (
	"context"
	"fmt"

	"godfs/internal/domain"
)

// CommitChunk validates inputs and records a successful chunk write.
func CommitChunk(ctx context.Context, store MasterStore, path string, chunkID domain.ChunkID, chunkIndex, chunkOffset, written int64, checksum []byte, version uint64) error {
	p, err := NormalizeFSPath(path)
	if err != nil {
		return err
	}
	if chunkID == "" {
		return fmt.Errorf("chunk_id required")
	}
	if written < 0 {
		return fmt.Errorf("invalid written")
	}
	if n := len(checksum); n != 0 && n != 32 {
		return fmt.Errorf("invalid checksum length")
	}
	return store.CommitChunk(ctx, p, chunkID, chunkIndex, chunkOffset, written, checksum, version)
}
