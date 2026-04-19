package usecase

import (
	"context"
	"time"

	"godfs/internal/domain"
)

// MasterStore defines the metadata operations required by the gRPC adapter.
// Implementations include the in-memory metadata.Store and the Raft-backed service.
type MasterStore interface {
	RegisterNode(ctx context.Context, n domain.ChunkNode) error

	Mkdir(ctx context.Context, path string) error
	CreateFile(ctx context.Context, path string) (domain.FileID, error)
	Delete(ctx context.Context, path string) ([]domain.ChunkDeleteInfo, error)
	Rename(ctx context.Context, oldPath, newPath string) error

	Stat(ctx context.Context, path string) (isDir bool, size int64, created, modified time.Time, mode uint32, err error)
	ListDir(ctx context.Context, path string) (names []string, ok bool, err error)

	PrepareWrite(ctx context.Context, path string, offset, length int64) (
		chunkID domain.ChunkID,
		primaryAddr string,
		secondaryAddrs []string,
		primaryNodeID domain.NodeID,
		leaseID domain.LeaseID,
		chunkIndex int64,
		chunkOff int64,
		chunkSize int64,
		version uint64,
		err error,
	)
	CommitChunk(ctx context.Context, path string, chunkID domain.ChunkID, chunkIndex, chunkOffset, written int64, checksum []byte, version uint64) error
	GetChunkForRead(ctx context.Context, path string, offset int64) (
		chunkID domain.ChunkID,
		replicaLocs []domain.ChunkReplica,
		chunkOff int64,
		available int64,
		version uint64,
		checksum []byte,
		err error,
	)

	// Heartbeat records liveness and capacity telemetry for a chunk node.
	// Implementations may persist it (e.g. via Raft) or keep it ephemeral.
	Heartbeat(ctx context.Context, nodeID domain.NodeID, capacityBytes, usedBytes int64) error

	// Snapshots (FR-10): consistent metadata manifests for backup tooling.
	CreateSnapshot(ctx context.Context, label string) (snapshotID string, createdAtUnix int64, err error)
	ListSnapshots(ctx context.Context) ([]domain.SnapshotInfo, error)
	GetSnapshot(ctx context.Context, snapshotID string) (*domain.BackupSnapshot, error)
	DeleteSnapshot(ctx context.Context, snapshotID string) error
}

