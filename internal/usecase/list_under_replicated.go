package usecase

import (
	"context"

	"godfs/internal/domain"
)

// ListUnderReplicatedChunks returns diagnostic entries for under-replicated chunks.
func ListUnderReplicatedChunks(ctx context.Context, store MasterStore, limit int) ([]domain.UnderReplicatedChunk, int, error) {
	if limit < 0 {
		limit = 0
	}
	return store.ListUnderReplicatedChunks(ctx, limit)
}
