package chunk

import (
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// FSStore stores chunk payloads as files under dataDir.
type FSStore struct {
	dataDir string
	mu      sync.Mutex
}

func NewFSStore(dataDir string) (*FSStore, error) {
	if err := os.MkdirAll(dataDir, 0o750); err != nil {
		return nil, err
	}
	return &FSStore{dataDir: dataDir}, nil
}

func (f *FSStore) path(chunkID string) string {
	return filepath.Join(f.dataDir, chunkID+".chk")
}

// WriteAt writes data at offset, extending file as needed.
func (f *FSStore) WriteAt(chunkID string, offset int64, data []byte) (written int64, sum []byte, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	p := f.path(chunkID)
	file, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return 0, nil, err
	}
	defer file.Close()

	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return 0, nil, err
	}
	n, err := file.Write(data)
	if err != nil {
		return int64(n), nil, err
	}
	if err := file.Sync(); err != nil {
		return int64(n), nil, err
	}

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return int64(n), nil, err
	}
	h := sha256.New()
	if _, err := io.Copy(h, file); err != nil {
		return int64(n), nil, err
	}
	return int64(n), h.Sum(nil), nil
}

// ReadAt reads up to len(buf) bytes at offset.
func (f *FSStore) ReadAt(chunkID string, offset int64, buf []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	p := f.path(chunkID)
	file, err := os.Open(p)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return 0, err
	}
	return file.Read(buf)
}

// Delete removes chunk file.
func (f *FSStore) Delete(chunkID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	err := os.Remove(f.path(chunkID))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

// Size returns current file size.
func (f *FSStore) Size(chunkID string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	fi, err := os.Stat(f.path(chunkID))
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// ReadAll returns the full chunk file (used by primary after write for replication).
func (f *FSStore) ReadAll(chunkID string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return os.ReadFile(f.path(chunkID))
}

// WriteFull replaces the chunk file contents (used by SyncChunk on secondaries).
func (f *FSStore) WriteFull(chunkID string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return os.WriteFile(f.path(chunkID), data, 0o640)
}
