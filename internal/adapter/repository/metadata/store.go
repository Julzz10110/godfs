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
	"godfs/internal/placement"
)

// Store is an in-memory metadata store.
type Store struct {
	mu sync.RWMutex

	chunkSize         int64
	replicationFactor int

	nodes []domain.ChunkNode
	// nodeUsedBytes is estimated bytes reserved per node (chunkSize per chunk placed).
	nodeUsedBytes map[domain.NodeID]int64
	nodeSet       map[domain.NodeID]int
	// placementRR advances after each successful Pick to rotate tie-breaking (see placement.Pick).
	placementRR int

	// In-memory master: node liveness, async delete-GC on chunk nodes, rebalance/heal (see also raftmeta.State).
	nodeDeadAfter  time.Duration
	nodeStatus     map[domain.NodeID]*nodeHBStatus
	pendingDeletes map[domain.ChunkID]map[string]*pendingChunkDelete
	rebalanceTasks map[domain.ChunkID]*rebalanceWork

	dirs  map[string]struct{}
	files map[string]*fileRec

	chunks map[domain.ChunkID]*chunkRec

	leaseDur time.Duration

	snapshots map[string]*domain.BackupSnapshot
}

type nodeHBStatus struct {
	LastSeen      time.Time
	CapacityBytes int64
	UsedBytes     int64
}

type pendingChunkDelete struct {
	CreatedUnix     int64
	Attempts        int
	NextAttemptUnix int64
}

type rebalanceWork struct {
	Attempts        int
	NextAttemptUnix int64
	LastError       string
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
	replicas []domain.ChunkReplica // [0] is primary
	version  uint64
	checksum []byte
	leaseID  domain.LeaseID
	leaseExp time.Time
}

// NewStore creates a metadata store. replication is the target replica count (default from config).
func NewStore(chunkSize int64, replication int) *Store {
	if chunkSize <= 0 {
		chunkSize = config.DefaultChunkSize
	}
	if replication <= 0 {
		replication = config.DefaultReplication
	}
	return &Store{
		chunkSize:         chunkSize,
		replicationFactor: replication,
		dirs: map[string]struct{}{
			"/": {},
		},
		files:          map[string]*fileRec{},
		chunks:         map[domain.ChunkID]*chunkRec{},
		nodeSet:        map[domain.NodeID]int{},
		nodeUsedBytes:  map[domain.NodeID]int64{},
		nodeStatus:     map[domain.NodeID]*nodeHBStatus{},
		pendingDeletes: map[domain.ChunkID]map[string]*pendingChunkDelete{},
		rebalanceTasks: map[domain.ChunkID]*rebalanceWork{},
		leaseDur:       time.Duration(config.DefaultLeaseSec) * time.Second,
		snapshots:      map[string]*domain.BackupSnapshot{},
	}
}

// SetNodeDeadAfter configures how long after the last heartbeat a node is treated as dead
// for placement and reads (0 = disable liveness filtering).
func (s *Store) SetNodeDeadAfter(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodeDeadAfter = d
}

// Heartbeat records liveness and disk telemetry for a chunk node.
func (s *Store) Heartbeat(_ context.Context, nodeID domain.NodeID, capacityBytes, usedBytes int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.nodeStatus[nodeID]
	if !ok {
		st = &nodeHBStatus{}
		s.nodeStatus[nodeID] = st
	}
	st.LastSeen = time.Now().UTC()
	st.CapacityBytes = capacityBytes
	st.UsedBytes = usedBytes
	return nil
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
	if _, ok := s.nodeUsedBytes[n.ID]; !ok {
		s.nodeUsedBytes[n.ID] = 0
	}
	if _, ok := s.nodeStatus[n.ID]; !ok {
		s.nodeStatus[n.ID] = &nodeHBStatus{}
	}
	return nil
}

func (s *Store) isAliveAt(id domain.NodeID, at time.Time) bool {
	st, ok := s.nodeStatus[id]
	if !ok || st.LastSeen.IsZero() || s.nodeDeadAfter <= 0 {
		return true
	}
	return !st.LastSeen.Add(s.nodeDeadAfter).Before(at)
}

func (s *Store) effectiveUsedForPlacementLocked() map[domain.NodeID]int64 {
	out := make(map[domain.NodeID]int64)
	for id, u := range s.nodeUsedBytes {
		out[id] = u
	}
	for id, st := range s.nodeStatus {
		if st != nil && st.UsedBytes > out[id] {
			out[id] = st.UsedBytes
		}
	}
	return out
}

// pickNodes chooses distinct chunk servers by free capacity (see [placement.Pick]).
// Caller must hold s.mu (write lock).
func (s *Store) pickNodes(n int) ([]domain.ChunkNode, error) {
	if len(s.nodes) == 0 {
		return nil, domain.ErrNoChunkServer
	}
	candidates := s.nodes
	if s.nodeDeadAfter > 0 {
		at := time.Now().UTC()
		var filtered []domain.ChunkNode
		for _, node := range s.nodes {
			if s.isAliveAt(node.ID, at) {
				filtered = append(filtered, node)
			}
		}
		candidates = filtered
	}
	if len(candidates) == 0 {
		return nil, domain.ErrNoChunkServer
	}
	out, err := placement.Pick(candidates, n, s.effectiveUsedForPlacementLocked(), s.placementRR)
	if err != nil {
		return nil, err
	}
	s.placementRR++
	return out, nil
}

func (s *Store) reserveChunkOnNodes(nodes []domain.ChunkNode) {
	for _, n := range nodes {
		s.nodeUsedBytes[n.ID] += s.chunkSize
	}
}

func (s *Store) releaseChunkFromReplicas(rep []domain.ChunkReplica) {
	for _, r := range rep {
		u := s.nodeUsedBytes[r.NodeID] - s.chunkSize
		if u < 0 {
			u = 0
		}
		s.nodeUsedBytes[r.NodeID] = u
	}
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

// Delete removes a file or empty directory. For files, returns replica locations for chunk GC.
func (s *Store) Delete(_ context.Context, p string) ([]domain.ChunkDeleteInfo, error) {
	fp, err := normalizePath(p)
	if err == nil {
		s.mu.Lock()
		if _, ok := s.files[fp]; ok {
			s.mu.Unlock()
			return s.deleteFile(fp)
		}
		if _, ok := s.dirs[fp]; ok {
			s.mu.Unlock()
			return s.deleteDir(fp)
		}
		s.mu.Unlock()
		return nil, domain.ErrNotFound
	}
	if d, err2 := normalizeDir(p); err2 == nil {
		return s.deleteDir(d)
	}
	return nil, err
}

func (s *Store) deleteFile(fp string) ([]domain.ChunkDeleteInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	fr, ok := s.files[fp]
	if !ok {
		return nil, domain.ErrNotFound
	}
	var infos []domain.ChunkDeleteInfo
	for _, cid := range fr.chunks {
		if cid == "" {
			continue
		}
		cr, ok := s.chunks[cid]
		if !ok {
			continue
		}
		addrs := make([]string, len(cr.replicas))
		for i, r := range cr.replicas {
			addrs[i] = r.Address
		}
		infos = append(infos, domain.ChunkDeleteInfo{
			ChunkID:  cid,
			Replicas: addrs,
		})
		if len(addrs) > 0 {
			set := s.pendingDeletes[cid]
			if set == nil {
				set = map[string]*pendingChunkDelete{}
				s.pendingDeletes[cid] = set
			}
			for _, a := range addrs {
				if _, ok := set[a]; !ok {
					set[a] = &pendingChunkDelete{CreatedUnix: time.Now().UTC().Unix()}
				}
			}
		}
		s.releaseChunkFromReplicas(cr.replicas)
		delete(s.chunks, cid)
	}
	delete(s.files, fp)
	return infos, nil
}

func (s *Store) deleteDir(d string) ([]domain.ChunkDeleteInfo, error) {
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
	oldP, errO := normalizePath(oldPath)
	newP, errN := normalizePath(newPath)
	if errO != nil {
		return errO
	}
	if errN != nil {
		return errN
	}
	s.mu.Lock()
	_, oldFile := s.files[oldP]
	_, oldDir := s.dirs[oldP]
	s.mu.Unlock()

	if oldFile {
		return s.renameFile(oldP, newP)
	}
	if oldDir {
		return s.renameDir(oldP, newP)
	}
	return domain.ErrNotFound
}

func (s *Store) renameFile(oldP, newP string) error {
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
	secondaryAddrs []string,
	primaryNodeID domain.NodeID,
	leaseID domain.LeaseID,
	chunkIndex int64,
	chunkOff int64,
	chunkSize int64,
	version uint64,
	err error,
) {
	fp, err := normalizePath(fpath)
	if err != nil {
		return "", "", nil, "", domain.LeaseID(""), 0, 0, 0, 0, err
	}
	if length <= 0 {
		return "", "", nil, "", domain.LeaseID(""), 0, 0, 0, 0, fmt.Errorf("invalid length")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	fr, ok := s.files[fp]
	if !ok {
		return "", "", nil, "", domain.LeaseID(""), 0, 0, 0, 0, domain.ErrNotFound
	}

	idx := offset / s.chunkSize
	chunkOff = offset % s.chunkSize
	if chunkOff+length > s.chunkSize {
		return "", "", nil, "", domain.LeaseID(""), 0, 0, 0, 0, fmt.Errorf("write crosses chunk boundary")
	}

	ensure := int(idx) + 1
	for len(fr.chunks) < ensure {
		fr.chunks = append(fr.chunks, "")
	}

	cid := fr.chunks[idx]
	if cid == "" {
		nodes, err := s.pickNodes(s.replicationFactor)
		if err != nil {
			return "", "", nil, "", domain.LeaseID(""), 0, 0, 0, 0, err
		}
		s.reserveChunkOnNodes(nodes)
		cid = domain.ChunkID(uuid.NewString())
		lid := domain.LeaseID(uuid.NewString())
		exp := time.Now().UTC().Add(s.leaseDur)
		replicas := make([]domain.ChunkReplica, len(nodes))
		for i := range nodes {
			replicas[i] = domain.ChunkReplica{NodeID: nodes[i].ID, Address: nodes[i].GRPCAddress}
		}
		s.chunks[cid] = &chunkRec{
			id:       cid,
			replicas: replicas,
			version:  1,
			leaseID:  lid,
			leaseExp: exp,
		}
		fr.chunks[idx] = cid
		var sec []string
		for i := 1; i < len(replicas); i++ {
			sec = append(sec, replicas[i].Address)
		}
		return cid, replicas[0].Address, sec, replicas[0].NodeID, lid, idx, chunkOff, s.chunkSize, 1, nil
	}

	cr, ok := s.chunks[cid]
	if !ok {
		return "", "", nil, "", domain.LeaseID(""), 0, 0, 0, 0, domain.ErrNotFound
	}
	lid := domain.LeaseID(uuid.NewString())
	cr.leaseID = lid
	cr.leaseExp = time.Now().UTC().Add(s.leaseDur)
	var sec []string
	for i := 1; i < len(cr.replicas); i++ {
		sec = append(sec, cr.replicas[i].Address)
	}
	return cid, cr.replicas[0].Address, sec, cr.replicas[0].NodeID, lid, idx, chunkOff, s.chunkSize, cr.version, nil
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
	replicaLocs []domain.ChunkReplica,
	chunkOff int64,
	available int64,
	version uint64,
	checksum []byte,
	err error,
) {
	fp, err := normalizePath(fpath)
	if err != nil {
		return "", nil, 0, 0, 0, nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	fr, ok := s.files[fp]
	if !ok {
		return "", nil, 0, 0, 0, nil, domain.ErrNotFound
	}
	if offset < 0 || offset >= fr.size {
		return "", nil, 0, 0, 0, nil, fmt.Errorf("offset out of range")
	}

	idx := offset / s.chunkSize
	chunkOff = offset % s.chunkSize
	if int(idx) >= len(fr.chunks) {
		return "", nil, 0, 0, 0, nil, domain.ErrNotFound
	}
	cid := fr.chunks[idx]
	if cid == "" {
		return "", nil, 0, 0, 0, nil, domain.ErrNotFound
	}
	cr, ok := s.chunks[cid]
	if !ok {
		return "", nil, 0, 0, 0, nil, domain.ErrNotFound
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

	var sum []byte
	if len(cr.checksum) > 0 {
		sum = append([]byte(nil), cr.checksum...)
	}
	reps := append([]domain.ChunkReplica(nil), cr.replicas...)
	if s.nodeDeadAfter > 0 && len(reps) > 1 {
		at := time.Now().UTC()
		var alive, dead []domain.ChunkReplica
		for _, r := range reps {
			if s.isAliveAt(r.NodeID, at) {
				alive = append(alive, r)
			} else {
				dead = append(dead, r)
			}
		}
		reps = append(alive, dead...)
	}
	return cid, reps, chunkOff, avail, cr.version, sum, nil
}
