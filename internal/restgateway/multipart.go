package restgateway

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const multipartManifestFile = "manifest.json"

// DefaultMultipartDataDir is the base directory for S3-style multipart staging (GODFS_REST_MULTIPART_DIR or os temp).
func DefaultMultipartDataDir() string {
	s := strings.TrimSpace(os.Getenv("GODFS_REST_MULTIPART_DIR"))
	if s != "" {
		return s
	}
	return filepath.Join(os.TempDir(), "godfs-rest-multipart")
}

func defaultMultipartMaxParts() int {
	const def = 1000
	s := strings.TrimSpace(os.Getenv("GODFS_REST_MULTIPART_MAX_PARTS"))
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 || n > 10000 {
		return def
	}
	return n
}

func defaultMultipartMaxPartBytes() int64 {
	s := strings.TrimSpace(os.Getenv("GODFS_REST_MULTIPART_MAX_PART_BYTES"))
	if s == "" {
		return DefaultMaxUploadBytes()
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 {
		return DefaultMaxUploadBytes()
	}
	return n
}

type multipartManifestJSON struct {
	Path string `json:"path"`
}

type multipartCompletePart struct {
	PartNumber int    `json:"part_number"`
	ETag       string `json:"etag,omitempty"`
}

type multipartCompleteBody struct {
	Parts []multipartCompletePart `json:"parts"`
}

type multipartManager struct {
	dir    string
	initMu sync.Mutex
	locks  sync.Map // uploadID -> *sync.Mutex
}

func newMultipartManager(dir string) *multipartManager {
	return &multipartManager{dir: dir}
}

func (m *multipartManager) uploadDir(uploadID string) string {
	return filepath.Join(m.dir, uploadID)
}

func (m *multipartManager) lockFor(uploadID string) *sync.Mutex {
	v, _ := m.locks.LoadOrStore(uploadID, &sync.Mutex{})
	return v.(*sync.Mutex)
}

func (m *multipartManager) Init(path string) (uploadID string, err error) {
	m.initMu.Lock()
	defer m.initMu.Unlock()
	if err := os.MkdirAll(m.dir, 0o700); err != nil {
		return "", err
	}
	var idb [16]byte
	if _, err := rand.Read(idb[:]); err != nil {
		return "", err
	}
	uploadID = hex.EncodeToString(idb[:])
	dir := m.uploadDir(uploadID)
	if err := os.Mkdir(dir, 0o700); err != nil {
		return "", err
	}
	b, err := json.Marshal(multipartManifestJSON{Path: path})
	if err != nil {
		_ = os.RemoveAll(dir)
		return "", err
	}
	if err := os.WriteFile(filepath.Join(dir, multipartManifestFile), b, 0o600); err != nil {
		_ = os.RemoveAll(dir)
		return "", err
	}
	return uploadID, nil
}

func (m *multipartManager) readManifest(uploadID string) (multipartManifestJSON, error) {
	b, err := os.ReadFile(filepath.Join(m.uploadDir(uploadID), multipartManifestFile))
	if err != nil {
		return multipartManifestJSON{}, err
	}
	var man multipartManifestJSON
	if err := json.Unmarshal(b, &man); err != nil {
		return multipartManifestJSON{}, err
	}
	if man.Path == "" {
		return multipartManifestJSON{}, errors.New("bad manifest")
	}
	return man, nil
}

// PutPart stores one part; returns raw etag token (unquoted), e.g. sha256-<hex>.
func (m *multipartManager) PutPart(uploadID string, partNum int, r io.Reader, maxPartBytes int64) (etag string, written int64, err error) {
	mu := m.lockFor(uploadID)
	mu.Lock()
	defer mu.Unlock()
	maxParts := defaultMultipartMaxParts()
	if partNum < 1 || partNum > maxParts {
		return "", 0, fmt.Errorf("part_number out of range")
	}
	dir := m.uploadDir(uploadID)
	if st, err := os.Stat(dir); err != nil || !st.IsDir() {
		return "", 0, os.ErrNotExist
	}
	fpath := filepath.Join(dir, fmt.Sprintf("part-%d", partNum))
	tmp := fpath + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return "", 0, err
	}
	h := sha256.New()
	w := io.MultiWriter(f, h)
	n, err := io.Copy(w, io.LimitReader(r, maxPartBytes+1))
	if err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return "", 0, err
	}
	if n > maxPartBytes {
		_ = f.Close()
		_ = os.Remove(tmp)
		return "", 0, fmt.Errorf("part too large")
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return "", 0, err
	}
	if err := os.Rename(tmp, fpath); err != nil {
		_ = os.Remove(tmp)
		return "", 0, err
	}
	sum := hex.EncodeToString(h.Sum(nil))
	return "sha256-" + sum, n, nil
}

func normalizeETag(s string) string {
	s = strings.TrimSpace(s)
	s = strings.Trim(s, `"`)
	if i := strings.Index(strings.ToLower(s), "sha256-"); i >= 0 {
		s = s[i+len("sha256-"):]
	}
	return strings.ToLower(s)
}

func verifyPartETag(uploadID string, part multipartCompletePart, dir string) error {
	if strings.TrimSpace(part.ETag) == "" {
		return nil
	}
	b, err := os.ReadFile(filepath.Join(dir, fmt.Sprintf("part-%d", part.PartNumber)))
	if err != nil {
		return err
	}
	h := sha256.Sum256(b)
	sum := hex.EncodeToString(h[:])
	want := normalizeETag(part.ETag)
	if want != sum {
		return fmt.Errorf("etag mismatch part %d", part.PartNumber)
	}
	return nil
}

type multiReadCloser struct {
	files []*os.File
	r     io.Reader
}

func (m *multiReadCloser) Read(p []byte) (int, error) {
	return m.r.Read(p)
}

func (m *multiReadCloser) Close() error {
	var first error
	for _, f := range m.files {
		if err := f.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func openOrderedParts(uploadID string, parts []multipartCompletePart, dir string) (io.ReadCloser, error) {
	if len(parts) == 0 {
		return nil, errors.New("no parts")
	}
	sorted := append([]multipartCompletePart(nil), parts...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].PartNumber < sorted[j].PartNumber })
	for i := range sorted {
		if sorted[i].PartNumber < 1 {
			return nil, errors.New("invalid part_number")
		}
		if i > 0 && sorted[i].PartNumber == sorted[i-1].PartNumber {
			return nil, errors.New("duplicate part_number")
		}
		if err := verifyPartETag(uploadID, sorted[i], dir); err != nil {
			return nil, err
		}
	}
	var files []*os.File
	var readers []io.Reader
	for _, p := range sorted {
		f, err := os.Open(filepath.Join(dir, fmt.Sprintf("part-%d", p.PartNumber)))
		if err != nil {
			for _, x := range files {
				_ = x.Close()
			}
			return nil, err
		}
		files = append(files, f)
		readers = append(readers, f)
	}
	return &multiReadCloser{files: files, r: io.MultiReader(readers...)}, nil
}

// errMultipartObjectExists signals HTTP 409 when completing without overwrite and path already exists.
var errMultipartObjectExists = errors.New("object already exists")

var errMultipartPathIsDir = errors.New("path is directory")

// Complete merges parts, writes to the final path via the gateway client, and removes staging.
func (m *multipartManager) Complete(ctx context.Context, cli gatewayClient, uploadID string, body multipartCompleteBody, overwrite bool) (finalPath string, err error) {
	mu := m.lockFor(uploadID)
	mu.Lock()
	defer mu.Unlock()

	dir := m.uploadDir(uploadID)
	if _, err := os.Stat(dir); err != nil {
		return "", fmt.Errorf("upload: %w", err)
	}
	man, err := m.readManifest(uploadID)
	if err != nil {
		return "", err
	}
	p := man.Path
	if okPath, ok := requirePath(p); ok {
		p = okPath
	} else {
		return "", errors.New("invalid path in manifest")
	}

	if !overwrite {
		st, err := cli.Stat(ctx, p)
		if err == nil {
			if st.IsDir {
				return "", errMultipartPathIsDir
			}
			return "", errMultipartObjectExists
		}
	} else {
		_ = cli.Delete(ctx, p)
	}

	rc, err := openOrderedParts(uploadID, body.Parts, dir)
	if err != nil {
		return "", err
	}
	defer rc.Close()

	if err := cli.Create(ctx, p); err != nil {
		return "", err
	}
	if err := cli.WriteFromReader(ctx, p, rc); err != nil {
		_ = cli.Delete(ctx, p)
		return "", err
	}
	_ = os.RemoveAll(dir)
	m.locks.Delete(uploadID)
	return p, nil
}

func (m *multipartManager) Abort(uploadID string) {
	mu := m.lockFor(uploadID)
	mu.Lock()
	defer mu.Unlock()
	_ = os.RemoveAll(m.uploadDir(uploadID))
	m.locks.Delete(uploadID)
}
