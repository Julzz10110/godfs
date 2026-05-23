package usecase

import (
	"context"
	"errors"
	"testing"
	"time"

	"godfs/internal/domain"
)

type fakeMasterStore struct {
	createSnapshot func(ctx context.Context, label string) (string, int64, error)
}

func (f *fakeMasterStore) RegisterNode(context.Context, domain.ChunkNode) error { return nil }
func (f *fakeMasterStore) Mkdir(context.Context, string) error                  { return nil }
func (f *fakeMasterStore) CreateFile(context.Context, string) (domain.FileID, error) {
	return "", nil
}

func (f *fakeMasterStore) Delete(context.Context, string) ([]domain.ChunkDeleteInfo, error) {
	return nil, nil
}
func (f *fakeMasterStore) RestoreFile(context.Context, string) error { return nil }
func (f *fakeMasterStore) Rename(context.Context, string, string) error {
	return nil
}

func (f *fakeMasterStore) Stat(context.Context, string) (bool, int64, time.Time, time.Time, uint32, error) {
	return false, 0, time.Time{}, time.Time{}, 0, nil
}

func (f *fakeMasterStore) ListDir(context.Context, string) ([]string, bool, error) {
	return nil, false, nil
}

func (f *fakeMasterStore) PrepareWrite(context.Context, string, int64, int64) (
	domain.ChunkID, string, []string, domain.NodeID, domain.LeaseID, int64, int64, int64, uint64, error,
) {
	return "", "", nil, "", "", 0, 0, 0, 0, nil
}

func (f *fakeMasterStore) CommitChunk(context.Context, string, domain.ChunkID, int64, int64, int64, []byte, uint64) error {
	return nil
}

func (f *fakeMasterStore) GetChunkForRead(context.Context, string, int64) (
	domain.ChunkID, []domain.ChunkReplica, int64, int64, uint64, []byte, error,
) {
	return "", nil, 0, 0, 0, nil, nil
}

func (f *fakeMasterStore) Heartbeat(context.Context, domain.NodeID, int64, int64) error {
	return nil
}

func (f *fakeMasterStore) CreateSnapshot(ctx context.Context, label string) (string, int64, error) {
	if f.createSnapshot != nil {
		return f.createSnapshot(ctx, label)
	}
	return "", 0, nil
}

func (f *fakeMasterStore) ListSnapshots(context.Context) ([]domain.SnapshotInfo, error) {
	return nil, nil
}

func (f *fakeMasterStore) GetSnapshot(context.Context, string) (*domain.BackupSnapshot, error) {
	return nil, nil
}
func (f *fakeMasterStore) DeleteSnapshot(context.Context, string) error { return nil }
func (f *fakeMasterStore) RestoreSnapshot(context.Context, *domain.BackupSnapshot, bool) error {
	return nil
}

func (f *fakeMasterStore) ListChunkNodes(context.Context) ([]domain.ChunkNodeDiag, error) {
	return nil, nil
}
func (f *fakeMasterStore) RunRebalanceSteps(context.Context, int) (int, error) { return 0, nil }
func (f *fakeMasterStore) ListUnderReplicatedChunks(context.Context, int) ([]domain.UnderReplicatedChunk, int, error) {
	return nil, 0, nil
}

func (f *fakeMasterStore) TruncateFile(context.Context, string, int64) ([]domain.ChunkDeleteInfo, error) {
	return nil, nil
}

func TestCreateSnapshot_invalidLabel(t *testing.T) {
	t.Parallel()
	_, _, err := CreateSnapshot(context.Background(), &fakeMasterStore{}, "bad/label")
	if !errors.Is(err, domain.ErrInvalidSnapshotLabel) {
		t.Fatalf("got %v", err)
	}
}

func TestCreateSnapshot_delegatesToStore(t *testing.T) {
	t.Parallel()
	var called bool
	store := &fakeMasterStore{
		createSnapshot: func(_ context.Context, label string) (string, int64, error) {
			called = true
			if label != "backup1" {
				t.Fatalf("label %q", label)
			}
			return "snap-1", 42, nil
		},
	}
	id, ts, err := CreateSnapshot(context.Background(), store, "backup1")
	if err != nil || !called || id != "snap-1" || ts != 42 {
		t.Fatalf("id=%q ts=%d err=%v called=%v", id, ts, err, called)
	}
}
