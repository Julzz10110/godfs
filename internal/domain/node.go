package domain

// ChunkNode is a registered chunk server.
type ChunkNode struct {
	ID            NodeID
	GRPCAddress   string
	CapacityBytes int64
}
