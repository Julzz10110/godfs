package usecase

import (
	"context"

	"godfs/internal/domain"
)

const maxRebalanceSteps = 200

// ListChunkNodes returns registered chunk nodes (admin diagnostic).
func ListChunkNodes(ctx context.Context, store MasterStore) ([]domain.ChunkNodeDiag, error) {
	return store.ListChunkNodes(ctx)
}

// RunRebalanceNow runs up to maxSteps rebalance iterations (admin).
func RunRebalanceNow(ctx context.Context, store MasterStore, maxSteps int) (int, error) {
	if maxSteps <= 0 {
		maxSteps = 1
	}
	if maxSteps > maxRebalanceSteps {
		maxSteps = maxRebalanceSteps
	}
	return store.RunRebalanceSteps(ctx, maxSteps)
}
