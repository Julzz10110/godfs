package usecase

import (
	"context"
	"fmt"

	"godfs/internal/domain"
)

// RegisterNode registers a chunk server with the metadata store.
func RegisterNode(ctx context.Context, store MasterStore, nodeID, grpcAddress string, capacityBytes int64) error {
	if nodeID == "" {
		return fmt.Errorf("node_id required")
	}
	if grpcAddress == "" {
		return fmt.Errorf("grpc_address required")
	}
	return store.RegisterNode(ctx, domain.ChunkNode{
		ID:            domain.NodeID(nodeID),
		GRPCAddress:   grpcAddress,
		CapacityBytes: capacityBytes,
	})
}
