package metadata

import (
	"context"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"godfs/internal/config"
	"godfs/internal/domain"
)

// Store is an in-memory metadata store (Phase 1 — no Raft).
type Store struct {
	mu sync.RWMutex

	chunkSize int64

	nodes   []domain.ChunkNode
	rr      int
	nodeSet map[domain.NodeID]int

	dirs  map[string]struct{}
	files map[string]*fileRec

	chunks map[domain.ChunkID]*chunkRec

	leaseDur time.Duration
}

type fileRec struct {
	id       domain.FileID
	chunks   []domain.ChunkID
	size     int64
	created  time.Time
	modified time.Time
	mode     uint32
}

type chunkRec struct {
	id       domain.ChunkID
	nodeID   domain.NodeID
	address  string
	version  uint64
	checksum []byte
	leaseID  domain.LeaseID
	leaseExp time.Time
}

// NewStore creates a metadata store.
func NewStore(chunkSize int64) *Store {
	if chunkSize <= 0 {
		chunkSize = config.DefaultChunkSize
	}
	return &Store{
		chunkSize: chunkSize,
		dirs: map[string]struct{}{
			"/": {},
		},
		files:    map[string]*fileRec{},
		chunks:   map[domain.ChunkID]*chunkRec{},
		nodeSet:  map[domain.NodeID]int{},
		leaseDur: time.Duration(config.DefaultLeaseSec) * time.Second,
	}
}

func normalizePath(p string) (string, error) {
	if p == "" || p[0] != '/' {
		return "", domain.ErrInvalidPath
	}
	c := path.Clean(p)
	if c == "/" {
		return "", domain.ErrInvalidPath
	}
	return c, nil
}

func normalizeDir(p string) (string, error) {
	if p == "" || p[0] != '/' {
		return "", domain.ErrInvalidPath
	}
	c := path.Clean(p)
	if c == "" {
		return "", domain.ErrInvalidPath
	}
	return c, nil
}

// RegisterNode adds or updates a chunk server.
func (s *Store) RegisterNode(_ context.Context, n domain.ChunkNode) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if idx, ok := s.nodeSet[n.ID]; ok {
		s.nodes[idx] = n
		return nil
	}
	s.nodeSet[n.ID] = len(s.nodes)
	s.nodes = append(s.nodes, n)
	return nil
}

func (s *Store) pickNode() (domain.ChunkNode, error) {
	if len(s.nodes) == 0 {
		return domain.ChunkNode{}, domain.ErrNoChunkServer
	}
	n := s.nodes[s.rr%len(s.nodes)]
	s.rr++
	return n, nil
}

// Mkdir creates one directory; parent must exist.
func (s *Store) Mkdir(_ context.Context, p string) error {
	dir, err := normalizeDir(p)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if dir == "/" {
		return domain.ErrAlreadyExists
	}
	if _, ok := s.files[dir]; ok {
		return domain.ErrAlreadyExists
	}
	if _, ok := s.dirs[dir]; ok {
		return domain.ErrAlreadyExists
	}
	parent := path.Dir(dir)
	if parent != "/" {
		if _, ok := s.dirs[parent]; !ok {
			return domain.ErrParentNotFound
		}
	}
	s.dirs[dir] = struct{}{}
	return nil
}

// CreateFile creates an empty file.
func (s *Store) CreateFile(_ context.Context, p string) (domain.FileID, error) {
	fp, err := normalizePath(p)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.dirs[fp]; ok {
		return "", domain.ErrAlreadyExists
	}
	if _, ok := s.files[fp]; ok {
		return "", domain.ErrAlreadyExists
	}
	parent := path.Dir(fp)
	if parent != "/" {
		if _, ok := s.dirs[parent]; !ok {
			return "", domain.ErrParentNotFound
		}
	}

	id := domain.FileID(uuid.NewString())
	now := time.Now().UTC()
	s.files[fp] = &fileRec{
		id:       id,
		chunks:   nil,
		size:     0,
		created:  now,
		modified: now,
		mode:     0o644,
	}
	return id, nil
}

// Delete removes a file or empty directory.
func (s *Store) Delete(_ context.Context, p string) ([]domain.ChunkID, error) {
	if d, err := normalizeDir(p); err == nil {
		return s.deleteDir(d)
	}
	fp, err := normalizePath(p)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	fr, ok := s.files[fp]
	if !ok {
		return nil, domain.ErrNotFound
	}
	chunks := append([]domain.ChunkID(nil), fr.chunks...)
	delete(s.files, fp)
	for _, cid := range chunks {
		if cid == "" {
			continue
		}
		delete(s.chunks, cid)
	}
	return chunks, nil
}

func (s *Store) deleteDir(d string) ([]domain.ChunkID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if d == "/" {
		return nil, domain.ErrInvalidPath
	}
	if _, ok := s.files[d]; ok {
		return nil, domain.ErrIsDir
	}
	if _, ok := s.dirs[d]; !ok {
		return nil, domain.ErrNotFound
	}
	prefix := d + "/"
	for f := range s.files {
		if f == d || strings.HasPrefix(f, prefix) {
			return nil, domain.ErrNotEmpty
		}
	}
	for dir := range s.dirs {
		if dir != d && strings.HasPrefix(dir, prefix) {
			return nil, domain.ErrNotEmpty
		}
	}
	delete(s.dirs, d)
	return nil, nil
}

// Rename atomically renames a file or directory.
func (s *Store) Rename(_ context.Context, oldPath, newPath string) error {
	if oldD, err := normalizeDir(oldPath); err == nil {
		newD, err2 := normalizeDir(newPath)
		if err2 != nil {
			return err2
		}
		return s.renameDir(oldD, newD)
	}
	oldP, err := normalizePath(oldPath)
	if err != nil {
		return err
	}
	newP, err := normalizePath(newPath)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	fr, ok := s.files[oldP]
	if !ok {
		return domain.ErrNotFound
	}
	parent := path.Dir(newP)
	if parent != "/" {
		if _, ok := s.dirs[parent]; !ok {
			return domain.ErrParentNotFound
		}
	}
	if _, ok := s.dirs[newP]; ok {
		return domain.ErrAlreadyExists
	}
	if _, ok := s.files[newP]; ok {
		return domain.ErrAlreadyExists
	}

	delete(s.files, oldP)
	s.files[newP] = fr
	fr.modified = time.Now().UTC()
	return nil
}

func (s *Store) renameDir(oldD, newD string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if oldD == "/" {
		return domain.ErrInvalidPath
	}
	if _, ok := s.dirs[oldD]; !ok {
		return domain.ErrNotFound
	}
	if _, ok := s.files[oldD]; ok {
		return domain.ErrNotFound
	}
	parent := path.Dir(newD)
	if parent != "/" {
		if _, ok := s.dirs[parent]; !ok {
			return domain.ErrParentNotFound
		}
	}
	if _, ok := s.dirs[newD]; ok {
		return domain.ErrAlreadyExists
	}
	if _, ok := s.files[newD]; ok {
		return domain.ErrAlreadyExists
	}

	prefix := oldD + "/"
	var movedDirs []string
	for d := range s.dirs {
		if d == oldD || strings.HasPrefix(d, prefix) {
			movedDirs = append(movedDirs, d)
		}
	}
	var movedFiles []string
	for f := range s.files {
		if strings.HasPrefix(f, prefix) {
			movedFiles = append(movedFiles, f)
		}
	}

	rel := func(full, rootOld, rootNew string) string {
		if full == rootOld {
			return rootNew
		}
		suf := strings.TrimPrefix(full, rootOld)
		return rootNew + suf
	}
	for _, d := range movedDirs {
		nd := rel(d, oldD, newD)
		delete(s.dirs, d)
		s.dirs[nd] = struct{}{}
	}
	for _, f := range movedFiles {
		nf := rel(f, oldD, newD)
		rec := s.files[f]
		delete(s.files, f)
		s.files[nf] = rec
	}
	return nil
}

// Stat returns metadata for path.
func (s *Store) Stat(_ context.Context, p string) (isDir bool, size int64, created, modified time.Time, mode uint32, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if d, e := normalizeDir(p); e == nil {
		if _, ok := s.dirs[d]; ok {
			return true, 0, time.Time{}, time.Time{}, 0o755, nil
		}
	}
	fp, err := normalizePath(p)
	if err != nil {
		return false, 0, time.Time{}, time.Time{}, 0, err
	}
	if _, ok := s.dirs[fp]; ok {
		return true, 0, time.Time{}, time.Time{}, 0o755, nil
	}
	fr, ok := s.files[fp]
	if !ok {
		return false, 0, time.Time{}, time.Time{}, 0, domain.ErrNotFound
	}
	return false, fr.size, fr.created, fr.modified, fr.mode, nil
}

// ListDir lists immediate children.
func (s *Store) ListDir(_ context.Context, p string) ([]string, bool, error) {
	dir, err := normalizeDir(p)
	if err != nil {
		return nil, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, ok := s.dirs[dir]; !ok {
		return nil, false, domain.ErrNotFound
	}

	seen := map[string]struct{}{}
	var names []string

	for d := range s.dirs {
		if d == dir {
			continue
		}
		if path.Dir(d) == dir {
			name := path.Base(d)
			if _, ok := seen[name]; !ok {
				seen[name] = struct{}{}
				names = append(names, name)
			}
		}
	}

	for f := range s.files {
		if path.Dir(f) == dir {
			name := path.Base(f)
			if _, ok := seen[name]; !ok {
				seen[name] = struct{}{}
				names = append(names, name)
			}
		}
	}

	return names, true, nil
}

// PrepareWrite allocates or returns chunk for write at file offset.
func (s *Store) PrepareWrite(_ context.Context, fpath string, offset, length int64) (
	chunkID domain.ChunkID,
	primaryAddr string,
	leaseID domain.LeaseID,
	chunkIndex int64,
	chunkOff int64,
	chunkSize int64,
	version uint64,
	err error,
) {
	fp, err := normalizePath(fpath)
	if err != nil {
		return "", "", "", 0, 0, 0, 0, err
	}
	if length <= 0 {
		return "", "", "", 0, 0, 0, 0, fmt.Errorf("invalid length")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	fr, ok := s.files[fp]
	if !ok {
		return "", "", "", 0, 0, 0, 0, domain.ErrNotFound
	}

	idx := offset / s.chunkSize
	chunkOff = offset % s.chunkSize
	if chunkOff+length > s.chunkSize {
		return "", "", "", 0, 0, 0, 0, fmt.Errorf("write crosses chunk boundary")
	}

	ensure := int(idx) + 1
	for len(fr.chunks) < ensure {
		fr.chunks = append(fr.chunks, "")
	}

	cid := fr.chunks[idx]
	if cid == "" {
		node, err := s.pickNode()
		if err != nil {
			return "", "", "", 0, 0, 0, 0, err
		}
		cid = domain.ChunkID(uuid.NewString())
		lid := domain.LeaseID(uuid.NewString())
		exp := time.Now().UTC().Add(s.leaseDur)
		s.chunks[cid] = &chunkRec{
			id:       cid,
			nodeID:   node.ID,
			address:  node.GRPCAddress,
			version:  1,
			leaseID:  lid,
			leaseExp: exp,
		}
		fr.chunks[idx] = cid
		return cid, node.GRPCAddress, lid, idx, chunkOff, s.chunkSize, 1, nil
	}

	cr, ok := s.chunks[cid]
	if !ok {
		return "", "", "", 0, 0, 0, 0, domain.ErrNotFound
	}
	lid := domain.LeaseID(uuid.NewString())
	cr.leaseID = lid
	cr.leaseExp = time.Now().UTC().Add(s.leaseDur)
	return cid, cr.address, lid, idx, chunkOff, s.chunkSize, cr.version, nil
}

// CommitChunk records successful write.
func (s *Store) CommitChunk(_ context.Context, fpath string, chunkID domain.ChunkID, chunkIndex, chunkOffset, written int64, checksum []byte, version uint64) error {
	fp, err := normalizePath(fpath)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	fr, ok := s.files[fp]
	if !ok {
		return domain.ErrNotFound
	}
	if int(chunkIndex) >= len(fr.chunks) || fr.chunks[chunkIndex] != chunkID {
		return domain.ErrChunkMismatch
	}
	cr, ok := s.chunks[chunkID]
	if !ok {
		return domain.ErrNotFound
	}
	if version != 0 && cr.version != version {
		return domain.ErrChunkMismatch
	}

	cr.version++
	cr.checksum = append([]byte(nil), checksum...)

	end := chunkIndex*s.chunkSize + chunkOffset + written
	if end > fr.size {
		fr.size = end
	}
	fr.modified = time.Now().UTC()
	return nil
}

// GetChunkForRead resolves chunk for a file offset.
func (s *Store) GetChunkForRead(_ context.Context, fpath string, offset int64) (
	chunkID domain.ChunkID,
	replicas []string,
	chunkOff int64,
	available int64,
	version uint64,
	err error,
) {
	fp, err := normalizePath(fpath)
	if err != nil {
		return "", nil, 0, 0, 0, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	fr, ok := s.files[fp]
	if !ok {
		return "", nil, 0, 0, 0, domain.ErrNotFound
	}
	if offset < 0 || offset >= fr.size {
		return "", nil, 0, 0, 0, fmt.Errorf("offset out of range")
	}

	idx := offset / s.chunkSize
	chunkOff = offset % s.chunkSize
	if int(idx) >= len(fr.chunks) {
		return "", nil, 0, 0, 0, domain.ErrNotFound
	}
	cid := fr.chunks[idx]
	if cid == "" {
		return "", nil, 0, 0, 0, domain.ErrNotFound
	}
	cr, ok := s.chunks[cid]
	if !ok {
		return "", nil, 0, 0, 0, domain.ErrNotFound
	}

	chunkStart := idx * s.chunkSize
	bytesInFileFromChunk := fr.size - chunkStart
	if bytesInFileFromChunk > s.chunkSize {
		bytesInFileFromChunk = s.chunkSize
	}
	avail := bytesInFileFromChunk - chunkOff
	if avail < 0 {
		avail = 0
	}

	return cid, []string{cr.address}, chunkOff, avail, cr.version, nil
}
