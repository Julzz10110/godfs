package domain

import "time"

// SnapshotInfo is a short listing entry for ListSnapshots.
type SnapshotInfo struct {
	ID            string
	Label         string
	CreatedAtUnix int64
	FileCount     int32
}

// BackupSnapshot is a point-in-time metadata manifest for offline backup (FR-10).
// Chunk bytes are not copied; replicas list helps target chunk servers.
type BackupSnapshot struct {
	ID                string
	Label             string
	CreatedAt         time.Time
	ChunkSize         int64
	ReplicationFactor int
	Files             []BackupFileEntry
}

// BackupFileEntry describes one file and its ordered chunk sequence.
type BackupFileEntry struct {
	Path       string
	Size       int64
	Mode       uint32
	CreatedAt  time.Time
	ModifiedAt time.Time
	Chunks     []BackupChunkRef
}

// BackupChunkRef references one chunk with replica hints.
type BackupChunkRef struct {
	ChunkID    ChunkID
	ChunkIndex int64
	Version    uint64
	Checksum   []byte
	Replicas   []ChunkReplica
}
