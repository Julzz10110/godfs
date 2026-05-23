package usecase

import (
	"context"
	"errors"
	"testing"
	"time"

	"godfs/internal/domain"
)

type mockDeleteStore struct {
	deleted       string
	err           error
	truncatePath  string
	truncateSize  int64
}

func (m *mockDeleteStore) Delete(ctx context.Context, path string) ([]domain.ChunkDeleteInfo, error) {
	m.deleted = path
	return nil, m.err
}

func (m *mockDeleteStore) RegisterNode(context.Context, domain.ChunkNode) error { return nil }
func (m *mockDeleteStore) Mkdir(context.Context, string) error                   { return nil }
func (m *mockDeleteStore) CreateFile(context.Context, string) (domain.FileID, error) {
	return "", nil
}
func (m *mockDeleteStore) RestoreFile(context.Context, string) error { return nil }
func (m *mockDeleteStore) Rename(context.Context, string, string) error {
	return nil
}
func (m *mockDeleteStore) Stat(context.Context, string) (bool, int64, time.Time, time.Time, uint32, error) {
	return false, 0, time.Time{}, time.Time{}, 0, nil
}
func (m *mockDeleteStore) ListDir(context.Context, string) ([]string, bool, error) {
	return nil, false, nil
}
func (m *mockDeleteStore) PrepareWrite(context.Context, string, int64, int64) (
	domain.ChunkID, string, []string, domain.NodeID, domain.LeaseID, int64, int64, int64, uint64, error,
) {
	return "", "", nil, "", "", 0, 0, 0, 0, nil
}
func (m *mockDeleteStore) CommitChunk(context.Context, string, domain.ChunkID, int64, int64, int64, []byte, uint64) error {
	return nil
}
func (m *mockDeleteStore) GetChunkForRead(context.Context, string, int64) (
	domain.ChunkID, []domain.ChunkReplica, int64, int64, uint64, []byte, error,
) {
	return "", nil, 0, 0, 0, nil, nil
}
func (m *mockDeleteStore) Heartbeat(context.Context, domain.NodeID, int64, int64) error { return nil }
func (m *mockDeleteStore) CreateSnapshot(context.Context, string) (string, int64, error) {
	return "", 0, nil
}
func (m *mockDeleteStore) ListSnapshots(context.Context) ([]domain.SnapshotInfo, error) {
	return nil, nil
}
func (m *mockDeleteStore) GetSnapshot(context.Context, string) (*domain.BackupSnapshot, error) {
	return nil, nil
}
func (m *mockDeleteStore) DeleteSnapshot(context.Context, string) error { return nil }
func (m *mockDeleteStore) RestoreSnapshot(context.Context, *domain.BackupSnapshot, bool) error {
	return nil
}
func (m *mockDeleteStore) ListChunkNodes(context.Context) ([]domain.ChunkNodeDiag, error) {
	return nil, nil
}
func (m *mockDeleteStore) RunRebalanceSteps(context.Context, int) (int, error) { return 0, nil }
func (m *mockDeleteStore) ListUnderReplicatedChunks(context.Context, int) ([]domain.UnderReplicatedChunk, int, error) {
	return nil, 0, nil
}
func (m *mockDeleteStore) TruncateFile(_ context.Context, path string, size int64) ([]domain.ChunkDeleteInfo, error) {
	m.truncatePath = path
	m.truncateSize = size
	return nil, nil
}

func TestDeleteFile_normalizesPath(t *testing.T) {
	m := &mockDeleteStore{}
	_, err := DeleteFile(context.Background(), m, "/foo/../bar")
	if err != nil {
		t.Fatal(err)
	}
	if m.deleted != "/bar" {
		t.Fatalf("deleted path %q want /bar", m.deleted)
	}
}

func TestDeleteFile_invalidPath(t *testing.T) {
	m := &mockDeleteStore{}
	_, err := DeleteFile(context.Background(), m, "relative")
	if !errors.Is(err, domain.ErrInvalidPath) {
		t.Fatalf("got %v want ErrInvalidPath", err)
	}
}
