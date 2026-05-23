package usecase

import (
	"context"
	"errors"
	"testing"
	"time"

	"godfs/internal/domain"
)

type mockFSOpsStore struct {
	registered    domain.ChunkNode
	heartbeatID   domain.NodeID
	mkdirPath     string
	createPath    string
	renameOld     string
	renameNew     string
	statPath      string
	listDirPath   string
	commitPath    string
	commitChunkID domain.ChunkID
	readPath      string
	readOffset    int64
}

func (m *mockFSOpsStore) RegisterNode(_ context.Context, n domain.ChunkNode) error {
	m.registered = n
	return nil
}

func (m *mockFSOpsStore) Heartbeat(_ context.Context, id domain.NodeID, _, _ int64) error {
	m.heartbeatID = id
	return nil
}

func (m *mockFSOpsStore) Mkdir(_ context.Context, path string) error {
	m.mkdirPath = path
	return nil
}

func (m *mockFSOpsStore) CreateFile(_ context.Context, path string) (domain.FileID, error) {
	m.createPath = path
	return domain.FileID("fid-1"), nil
}

func (m *mockFSOpsStore) Rename(_ context.Context, oldPath, newPath string) error {
	m.renameOld, m.renameNew = oldPath, newPath
	return nil
}

func (m *mockFSOpsStore) Stat(_ context.Context, path string) (bool, int64, time.Time, time.Time, uint32, error) {
	m.statPath = path
	return path == "/d", 0, time.Time{}, time.Time{}, 0o755, nil
}

func (m *mockFSOpsStore) ListDir(_ context.Context, path string) ([]string, bool, error) {
	m.listDirPath = path
	return []string{"a.txt", "sub"}, true, nil
}

func (m *mockFSOpsStore) CommitChunk(_ context.Context, path string, cid domain.ChunkID, _, _, _ int64, _ []byte, _ uint64) error {
	m.commitPath, m.commitChunkID = path, cid
	return nil
}

func (m *mockFSOpsStore) GetChunkForRead(_ context.Context, path string, offset int64) (
	domain.ChunkID, []domain.ChunkReplica, int64, int64, uint64, []byte, error,
) {
	m.readPath, m.readOffset = path, offset
	return "c1", []domain.ChunkReplica{{NodeID: "n1", Address: "127.0.0.1:1"}}, 0, 64, 1, nil, nil
}

func (m *mockFSOpsStore) Delete(context.Context, string) ([]domain.ChunkDeleteInfo, error) {
	return nil, nil
}

func (m *mockFSOpsStore) TruncateFile(context.Context, string, int64) ([]domain.ChunkDeleteInfo, error) {
	return nil, nil
}

func (m *mockFSOpsStore) RestoreFile(context.Context, string) error { return nil }

func (m *mockFSOpsStore) PrepareWrite(context.Context, string, int64, int64) (
	domain.ChunkID, string, []string, domain.NodeID, domain.LeaseID, int64, int64, int64, uint64, error,
) {
	return "", "", nil, "", "", 0, 0, 0, 0, nil
}

func (m *mockFSOpsStore) CreateSnapshot(context.Context, string) (string, int64, error) {
	return "", 0, nil
}

func (m *mockFSOpsStore) ListSnapshots(context.Context) ([]domain.SnapshotInfo, error) {
	return nil, nil
}

func (m *mockFSOpsStore) GetSnapshot(context.Context, string) (*domain.BackupSnapshot, error) {
	return nil, nil
}

func (m *mockFSOpsStore) DeleteSnapshot(context.Context, string) error { return nil }

func (m *mockFSOpsStore) RestoreSnapshot(context.Context, *domain.BackupSnapshot, bool) error {
	return nil
}

func (m *mockFSOpsStore) ListChunkNodes(context.Context) ([]domain.ChunkNodeDiag, error) {
	return nil, nil
}

func (m *mockFSOpsStore) RunRebalanceSteps(context.Context, int) (int, error) { return 0, nil }

func (m *mockFSOpsStore) ListUnderReplicatedChunks(context.Context, int) ([]domain.UnderReplicatedChunk, int, error) {
	return nil, 0, nil
}

func TestRegisterNode_validatesAndCallsStore(t *testing.T) {
	m := &mockFSOpsStore{}
	if err := RegisterNode(context.Background(), m, "n1", "127.0.0.1:9", 1<<30); err != nil {
		t.Fatal(err)
	}
	if m.registered.ID != "n1" || m.registered.GRPCAddress != "127.0.0.1:9" {
		t.Fatalf("registered %+v", m.registered)
	}
	if err := RegisterNode(context.Background(), m, "", "x", 0); err == nil {
		t.Fatal("expected node_id error")
	}
}

func TestHeartbeat_validatesNodeID(t *testing.T) {
	m := &mockFSOpsStore{}
	if err := Heartbeat(context.Background(), m, "n1", 1, 0); err != nil {
		t.Fatal(err)
	}
	if m.heartbeatID != "n1" {
		t.Fatalf("heartbeat %q", m.heartbeatID)
	}
	if err := Heartbeat(context.Background(), m, "", 0, 0); err == nil {
		t.Fatal("expected error")
	}
}

func TestCreateFile_normalizesPath(t *testing.T) {
	m := &mockFSOpsStore{}
	if _, err := CreateFile(context.Background(), m, "/a/../b"); err != nil {
		t.Fatal(err)
	}
	if m.createPath != "/b" {
		t.Fatalf("path %q", m.createPath)
	}
}

func TestMkdir_allowsRoot(t *testing.T) {
	m := &mockFSOpsStore{}
	if err := Mkdir(context.Background(), m, "/"); err != nil {
		t.Fatal(err)
	}
	if m.mkdirPath != "/" {
		t.Fatalf("path %q", m.mkdirPath)
	}
}

func TestRename_normalizesPaths(t *testing.T) {
	m := &mockFSOpsStore{}
	if err := Rename(context.Background(), m, "/x/../a", "/b/../c"); err != nil {
		t.Fatal(err)
	}
	if m.renameOld != "/a" || m.renameNew != "/c" {
		t.Fatalf("rename %q -> %q", m.renameOld, m.renameNew)
	}
	if err := Rename(context.Background(), m, "/", "/b"); !errors.Is(err, domain.ErrInvalidPath) {
		t.Fatalf("got %v", err)
	}
}

func TestStat_andListDir(t *testing.T) {
	m := &mockFSOpsStore{}
	info, err := Stat(context.Background(), m, "/d")
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir || m.statPath != "/d" {
		t.Fatalf("stat %+v path %q", info, m.statPath)
	}
	entries, err := ListDir(context.Background(), m, "/d")
	if err != nil || len(entries) != 2 {
		t.Fatalf("list: %v len=%d", err, len(entries))
	}
	if m.listDirPath != "/d" {
		t.Fatalf("list path %q", m.listDirPath)
	}
}

func TestCommitChunk_andGetChunkForRead(t *testing.T) {
	m := &mockFSOpsStore{}
	if err := CommitChunk(context.Background(), m, "/f", "c1", 0, 0, 10, nil, 0); err != nil {
		t.Fatal(err)
	}
	if m.commitPath != "/f" || m.commitChunkID != "c1" {
		t.Fatalf("commit %q %q", m.commitPath, m.commitChunkID)
	}
	plan, err := GetChunkForRead(context.Background(), m, "/f", 0)
	if err != nil {
		t.Fatal(err)
	}
	if plan.ChunkID != "c1" || m.readPath != "/f" {
		t.Fatalf("read plan %+v path %q", plan, m.readPath)
	}
	if _, err := GetChunkForRead(context.Background(), m, "/f", -1); err == nil {
		t.Fatal("expected offset error")
	}
}
