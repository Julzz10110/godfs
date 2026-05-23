package usecase

import (
	"context"

	"godfs/internal/domain"
)

// CreateFile validates the path and creates an empty file in metadata.
func CreateFile(ctx context.Context, store MasterStore, path string) (domain.FileID, error) {
	p, err := NormalizeFSPath(path)
	if err != nil {
		return "", err
	}
	return store.CreateFile(ctx, p)
}
