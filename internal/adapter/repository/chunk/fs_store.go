package chunk

import (
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"godfs/internal/domain"
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

func validateChunkFileID(id string) error {
	if id == "" || id == "." || id == ".." {
		return domain.ErrInvalidPath
	}
	if id != filepath.Base(id) {
		return domain.ErrInvalidPath
	}
	if len(id) > 512 {
		return domain.ErrInvalidPath
	}
	return nil
}

// resolvedChunkPath returns an absolute path to the chunk file under dataDir,
// rejecting traversal (e.g. chunk IDs containing path separators).
func (f *FSStore) resolvedChunkPath(chunkID string) (string, error) {
	if err := validateChunkFileID(chunkID); err != nil {
		return "", err
	}
	full := filepath.Join(f.dataDir, chunkID+".chk")
	baseAbs, err := filepath.Abs(f.dataDir)
	if err != nil {
		return "", err
	}
	fullAbs, err := filepath.Abs(full)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(baseAbs, fullAbs)
	if err != nil {
		return "", domain.ErrInvalidPath
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", domain.ErrInvalidPath
	}
	return fullAbs, nil
}

// WriteAt writes data at offset, extending file as needed.
func (f *FSStore) WriteAt(chunkID string, offset int64, data []byte) (written int64, sum []byte, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	p, err := f.resolvedChunkPath(chunkID)
	if err != nil {
		return 0, nil, err
	}
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

	p, err := f.resolvedChunkPath(chunkID)
	if err != nil {
		return 0, err
	}
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
	p, err := f.resolvedChunkPath(chunkID)
	if err != nil {
		return err
	}
	err = os.Remove(p)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

// Size returns current file size.
func (f *FSStore) Size(chunkID string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	p, err := f.resolvedChunkPath(chunkID)
	if err != nil {
		return 0, err
	}
	fi, err := os.Stat(p)
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// ReadAll returns the full chunk file (used by primary after write for replication).
func (f *FSStore) ReadAll(chunkID string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, err := f.resolvedChunkPath(chunkID)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(p)
}

func (f *FSStore) Checksum(chunkID string) (sum []byte, size int64, mod time.Time, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, err := f.resolvedChunkPath(chunkID)
	if err != nil {
		return nil, 0, time.Time{}, err
	}
	fi, err := os.Stat(p)
	if err != nil {
		return nil, 0, time.Time{}, err
	}
	b, err := os.ReadFile(p)
	if err != nil {
		return nil, 0, time.Time{}, err
	}
	h := sha256.Sum256(b)
	return h[:], fi.Size(), fi.ModTime().UTC(), nil
}

// WriteFull replaces the chunk file contents (used by SyncChunk on secondaries).
func (f *FSStore) WriteFull(chunkID string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, err := f.resolvedChunkPath(chunkID)
	if err != nil {
		return err
	}
	return os.WriteFile(p, data, 0o640)
}

// ListChunkIDs returns all chunk IDs currently stored on disk (best-effort).
func (f *FSStore) ListChunkIDs() ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	entries, err := os.ReadDir(f.dataDir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".chk") {
			continue
		}
		out = append(out, strings.TrimSuffix(name, ".chk"))
	}
	return out, nil
}

// ListChunks returns chunk IDs with file modification times.
func (f *FSStore) ListChunks() (map[string]time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	entries, err := os.ReadDir(f.dataDir)
	if err != nil {
		return nil, err
	}
	out := map[string]time.Time{}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".chk") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		out[strings.TrimSuffix(name, ".chk")] = info.ModTime().UTC()
	}
	return out, nil
}

// TotalChunkBytes returns the sum of *.chk file sizes under dataDir (best-effort for telemetry).
func (f *FSStore) TotalChunkBytes() (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	entries, err := os.ReadDir(f.dataDir)
	if err != nil {
		return 0, err
	}
	var sum int64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if !strings.HasSuffix(e.Name(), ".chk") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		sum += info.Size()
	}
	return sum, nil
}
