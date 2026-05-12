package domain

// ChunkNode is a registered chunk server.
type ChunkNode struct {
	ID            NodeID
	GRPCAddress   string
	CapacityBytes int64
}

// ChunkNodeDiag is operator-facing telemetry for a registered chunk node (best-effort).
type ChunkNodeDiag struct {
	ID            NodeID
	GRPCAddress   string
	CapacityBytes int64
	UsedBytes     int64
	LastSeenUnix  int64
	Alive         bool
}
