package domain

import "time"

// Chunk metadata on the master (MVP: single primary replica per chunk).
type Chunk struct {
	ID        ChunkID
	Size      int64
	Checksum  []byte
	Replicas  []NodeID
	Primary   NodeID
	Lease     *Lease
	Version   uint64
}

// Lease grants exclusive write to one client for a short period.
type Lease struct {
	ID        LeaseID
	Holder    string // client id — MVP uses fixed token
	ExpiresAt time.Time
	ChunkID   ChunkID
}
