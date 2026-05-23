package usecase

import (
	"context"
	"fmt"

	"godfs/internal/domain"
)

// Heartbeat records chunk node liveness and capacity telemetry.
func Heartbeat(ctx context.Context, store MasterStore, nodeID string, capacityBytes, usedBytes int64) error {
	if nodeID == "" {
		return fmt.Errorf("node_id required")
	}
	return store.Heartbeat(ctx, domain.NodeID(nodeID), capacityBytes, usedBytes)
}
