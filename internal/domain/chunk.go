package domain

import "time"

// ChunkReplica identifies one replica placement (node + gRPC endpoint).
type ChunkReplica struct {
	NodeID  NodeID
	Address string // gRPC address as registered with the master
}

// Chunk is metadata for a chunk on the master (replication group).
type Chunk struct {
	ID        ChunkID
	Size      int64
	Checksum  []byte
	Replicas  []ChunkReplica // [0] is primary
	Primary   NodeID
	Lease     *Lease
	Version   uint64
}

// Lease grants exclusive write to one client for a short period.
type Lease struct {
	ID        LeaseID
	Holder    string // client id — MVP uses opaque lease token
	ExpiresAt time.Time
	ChunkID   ChunkID
}
