package usecase

import (
	"context"
	"time"

	"godfs/internal/domain"
)

// mockMasterStore is a test double for MasterStore.
type mockMasterStore struct {
	createSnapshotLabel string
	restoreForce        bool
	restoreManifest     *domain.BackupSnapshot
	deleteSnapshotID    string
	restoreFilePath     string
	truncatePath        string
	truncateSize        int64
	rebalanceSteps      int
}

func (m *mockMasterStore) RegisterNode(context.Context, domain.ChunkNode) error { return nil }
func (m *mockMasterStore) Mkdir(context.Context, string) error                  { return nil }
func (m *mockMasterStore) CreateFile(context.Context, string) (domain.FileID, error) {
	return "", nil
}

func (m *mockMasterStore) Delete(context.Context, string) ([]domain.ChunkDeleteInfo, error) {
	return nil, nil
}

func (m *mockMasterStore) TruncateFile(_ context.Context, path string, size int64) ([]domain.ChunkDeleteInfo, error) {
	m.truncatePath = path
	m.truncateSize = size
	return nil, nil
}

func (m *mockMasterStore) RestoreFile(_ context.Context, path string) error {
	m.restoreFilePath = path
	return nil
}
func (m *mockMasterStore) Rename(context.Context, string, string) error { return nil }
func (m *mockMasterStore) Stat(context.Context, string) (bool, int64, time.Time, time.Time, uint32, error) {
	return false, 0, time.Time{}, time.Time{}, 0, nil
}

func (m *mockMasterStore) ListDir(context.Context, string) ([]string, bool, error) {
	return nil, false, nil
}

func (m *mockMasterStore) PrepareWrite(context.Context, string, int64, int64) (
	domain.ChunkID, string, []string, domain.NodeID, domain.LeaseID, int64, int64, int64, uint64, error,
) {
	return "", "", nil, "", "", 0, 0, 0, 0, nil
}

func (m *mockMasterStore) CommitChunk(context.Context, string, domain.ChunkID, int64, int64, int64, []byte, uint64) error {
	return nil
}

func (m *mockMasterStore) GetChunkForRead(context.Context, string, int64) (
	domain.ChunkID, []domain.ChunkReplica, int64, int64, uint64, []byte, error,
) {
	return "", nil, 0, 0, 0, nil, nil
}
func (m *mockMasterStore) Heartbeat(context.Context, domain.NodeID, int64, int64) error { return nil }
func (m *mockMasterStore) CreateSnapshot(_ context.Context, label string) (string, int64, error) {
	m.createSnapshotLabel = label
	return "snap-1", 1, nil
}

func (m *mockMasterStore) ListSnapshots(context.Context) ([]domain.SnapshotInfo, error) {
	return nil, nil
}

func (m *mockMasterStore) GetSnapshot(context.Context, string) (*domain.BackupSnapshot, error) {
	return nil, nil
}

func (m *mockMasterStore) DeleteSnapshot(_ context.Context, id string) error {
	m.deleteSnapshotID = id
	return nil
}

func (m *mockMasterStore) RestoreSnapshot(_ context.Context, man *domain.BackupSnapshot, force bool) error {
	m.restoreManifest = man
	m.restoreForce = force
	return nil
}

func (m *mockMasterStore) ListChunkNodes(context.Context) ([]domain.ChunkNodeDiag, error) {
	return nil, nil
}

func (m *mockMasterStore) RunRebalanceSteps(_ context.Context, maxSteps int) (int, error) {
	m.rebalanceSteps = maxSteps
	return maxSteps, nil
}

func (m *mockMasterStore) ListUnderReplicatedChunks(context.Context, int) ([]domain.UnderReplicatedChunk, int, error) {
	return nil, 0, nil
}
