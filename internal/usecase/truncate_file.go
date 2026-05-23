package usecase

import (
	"context"
	"fmt"

	"godfs/internal/domain"
)

// TruncateFile validates the path and sets file size in metadata.
func TruncateFile(ctx context.Context, store MasterStore, path string, size int64) ([]domain.ChunkDeleteInfo, error) {
	p, err := NormalizeFSPath(path)
	if err != nil {
		return nil, err
	}
	if size < 0 {
		return nil, fmt.Errorf("invalid size")
	}
	return store.TruncateFile(ctx, p, size)
}
