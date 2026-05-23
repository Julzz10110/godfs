package usecase

import (
	"context"

	"godfs/internal/domain"
)

// DeleteFile validates the path and removes file metadata (chunk GC is separate).
func DeleteFile(ctx context.Context, store MasterStore, path string) ([]domain.ChunkDeleteInfo, error) {
	p, err := NormalizeFSPath(path)
	if err != nil {
		return nil, err
	}
	return store.Delete(ctx, p)
}
