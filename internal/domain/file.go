package domain

import "time"

// ReplicaConfig holds replication policy (replication factor only; no per-path overrides yet).
type ReplicaConfig struct {
	Factor int
}

func DefaultReplicaConfig() ReplicaConfig {
	return ReplicaConfig{Factor: 3}
}

// File is namespace metadata for a file.
type File struct {
	ID            FileID
	Path          string
	ParentDirPath string
	ChunkIDs      []ChunkID // index i covers file bytes [i*chunkSize, (i+1)*chunkSize)
	Size          int64
	CreatedAt     time.Time
	ModifiedAt    time.Time
	Mode          uint32
	Replication   ReplicaConfig
}

// Dir describes a directory inode (path key in store).
type Dir struct {
	Path      string
	CreatedAt time.Time
	Mode      uint32
}
