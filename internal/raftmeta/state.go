package raftmeta

import (
	"fmt"
	"path"
	"strings"
	"time"

	"godfs/internal/domain"
	"godfs/internal/placement"
)

// State is the authoritative metadata state replicated by Raft.
// It is intentionally similar to metadata.Store but without mutexes and without non-deterministic ID generation.
type State struct {
	ChunkSize         int64
	ReplicationFactor int
	LeaseDur          time.Duration

	Nodes        []domain.ChunkNode
	NodeUsedBytes map[domain.NodeID]int64
	NodeSet       map[domain.NodeID]int // node id -> index in Nodes
	PlacementRR   int

	Dirs  map[string]struct{}
	Files map[string]*fileRec
	Chunks map[domain.ChunkID]*chunkRec
}

type fileRec struct {
	ID       domain.FileID
	Chunks   []domain.ChunkID
	Size     int64
	Created  time.Time
	Modified time.Time
	Mode     uint32
}

type chunkRec struct {
	ID       domain.ChunkID
	Replicas []domain.ChunkReplica // [0] is primary
	Version  uint64
	Checksum []byte
	LeaseID  domain.LeaseID
	LeaseExp time.Time
}

func NewState(chunkSize int64, replication int, leaseDur time.Duration) *State {
	if chunkSize <= 0 {
		panic("chunkSize must be > 0")
	}
	if replication <= 0 {
		replication = 1
	}
	if leaseDur <= 0 {
		leaseDur = 60 * time.Second
	}
	return &State{
		ChunkSize:         chunkSize,
		ReplicationFactor: replication,
		LeaseDur:          leaseDur,
		Dirs: map[string]struct{}{
			"/": {},
		},
		Files:         map[string]*fileRec{},
		Chunks:        map[domain.ChunkID]*chunkRec{},
		NodeSet:       map[domain.NodeID]int{},
		NodeUsedBytes: map[domain.NodeID]int64{},
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

func parentDir(p string) string {
	d := path.Dir(p)
	if d == "." {
		return "/"
	}
	return d
}

func (s *State) RegisterNode(n domain.ChunkNode) error {
	if idx, ok := s.NodeSet[n.ID]; ok {
		s.Nodes[idx] = n
		return nil
	}
	s.NodeSet[n.ID] = len(s.Nodes)
	s.Nodes = append(s.Nodes, n)
	if _, ok := s.NodeUsedBytes[n.ID]; !ok {
		s.NodeUsedBytes[n.ID] = 0
	}
	return nil
}

func (s *State) pickNodes(n int) ([]domain.ChunkNode, error) {
	if len(s.Nodes) == 0 {
		return nil, domain.ErrNoChunkServer
	}
	out, err := placement.Pick(s.Nodes, n, s.NodeUsedBytes, s.PlacementRR)
	if err != nil {
		return nil, err
	}
	s.PlacementRR++
	return out, nil
}

func (s *State) reserveChunkOnNodes(nodes []domain.ChunkNode) {
	for _, n := range nodes {
		s.NodeUsedBytes[n.ID] += s.ChunkSize
	}
}

func (s *State) releaseChunkFromReplicas(rep []domain.ChunkReplica) {
	for _, r := range rep {
		u := s.NodeUsedBytes[r.NodeID] - s.ChunkSize
		if u < 0 {
			u = 0
		}
		s.NodeUsedBytes[r.NodeID] = u
	}
}

func (s *State) Mkdir(p string) error {
	dir, err := normalizeDir(p)
	if err != nil {
		return err
	}
	if _, ok := s.Dirs[dir]; ok {
		return domain.ErrAlreadyExists
	}
	par := parentDir(dir)
	if _, ok := s.Dirs[par]; !ok {
		return domain.ErrParentNotFound
	}
	s.Dirs[dir] = struct{}{}
	return nil
}

func (s *State) CreateFile(p string, id domain.FileID) (domain.FileID, error) {
	fp, err := normalizePath(p)
	if err != nil {
		return "", err
	}
	if _, ok := s.Files[fp]; ok {
		return "", domain.ErrAlreadyExists
	}
	par := parentDir(fp)
	if _, ok := s.Dirs[par]; !ok {
		return "", domain.ErrParentNotFound
	}
	now := time.Now().UTC()
	s.Files[fp] = &fileRec{
		ID:       id,
		Chunks:   nil,
		Size:     0,
		Created:  now,
		Modified: now,
		Mode:     0,
	}
	return id, nil
}

func (s *State) Rename(oldPath, newPath string) error {
	// Mirror metadata.Store behavior: file rename if old is file, else dir rename.
	oldP, err := normalizeDir(oldPath)
	if err != nil {
		return err
	}
	newP, err := normalizeDir(newPath)
	if err != nil {
		return err
	}
	if fr, ok := s.Files[oldP]; ok {
		if _, ok := s.Files[newP]; ok {
			return domain.ErrAlreadyExists
		}
		par := parentDir(newP)
		if _, ok := s.Dirs[par]; !ok {
			return domain.ErrParentNotFound
		}
		delete(s.Files, oldP)
		s.Files[newP] = fr
		return nil
	}
	// directory rename
	if _, ok := s.Dirs[oldP]; !ok {
		return domain.ErrNotFound
	}
	if _, ok := s.Dirs[newP]; ok {
		return domain.ErrAlreadyExists
	}
	par := parentDir(newP)
	if _, ok := s.Dirs[par]; !ok {
		return domain.ErrParentNotFound
	}
	// Rename all nested dirs/files with prefix oldP + "/"
	oldPrefix := oldP
	if oldPrefix != "/" {
		oldPrefix += "/"
	}
	newPrefix := newP
	if newPrefix != "/" {
		newPrefix += "/"
	}
	// dirs
	for d := range s.Dirs {
		if d == oldP {
			continue
		}
		if strings.HasPrefix(d, oldPrefix) {
			nd := newPrefix + strings.TrimPrefix(d, oldPrefix)
			s.Dirs[nd] = struct{}{}
			delete(s.Dirs, d)
		}
	}
	delete(s.Dirs, oldP)
	s.Dirs[newP] = struct{}{}
	// files
	for fp, fr := range s.Files {
		if strings.HasPrefix(fp, oldPrefix) {
			nf := newPrefix + strings.TrimPrefix(fp, oldPrefix)
			s.Files[nf] = fr
			delete(s.Files, fp)
		}
	}
	return nil
}

func (s *State) Stat(p string) (isDir bool, size int64, created, modified time.Time, mode uint32, err error) {
	if p == "/" {
		if _, ok := s.Dirs["/"]; ok {
			return true, 0, time.Time{}, time.Time{}, 0, nil
		}
		return false, 0, time.Time{}, time.Time{}, 0, domain.ErrNotFound
	}
	dir, derr := normalizeDir(p)
	if derr == nil {
		if _, ok := s.Dirs[dir]; ok {
			return true, 0, time.Time{}, time.Time{}, 0, nil
		}
	}
	fp, err := normalizePath(p)
	if err != nil {
		return false, 0, time.Time{}, time.Time{}, 0, err
	}
	fr, ok := s.Files[fp]
	if !ok {
		return false, 0, time.Time{}, time.Time{}, 0, domain.ErrNotFound
	}
	return false, fr.Size, fr.Created, fr.Modified, fr.Mode, nil
}

func (s *State) ListDir(p string) ([]string, bool, error) {
	dir, err := normalizeDir(p)
	if err != nil {
		return nil, false, err
	}
	if _, ok := s.Dirs[dir]; !ok {
		return nil, false, domain.ErrNotFound
	}
	prefix := dir
	if prefix != "/" {
		prefix += "/"
	}
	seen := map[string]struct{}{}
	var names []string
	for d := range s.Dirs {
		if d == dir {
			continue
		}
		if strings.HasPrefix(d, prefix) {
			rest := strings.TrimPrefix(d, prefix)
			if rest == "" || strings.Contains(rest, "/") {
				continue
			}
			if _, ok := seen[rest]; ok {
				continue
			}
			seen[rest] = struct{}{}
			names = append(names, rest)
		}
	}
	for fp := range s.Files {
		if strings.HasPrefix(fp, prefix) {
			rest := strings.TrimPrefix(fp, prefix)
			if rest == "" || strings.Contains(rest, "/") {
				continue
			}
			if _, ok := seen[rest]; ok {
				continue
			}
			seen[rest] = struct{}{}
			names = append(names, rest)
		}
	}
	return names, true, nil
}

type PrepareWriteResult struct {
	ChunkID         domain.ChunkID
	PrimaryAddr     string
	SecondaryAddrs  []string
	PrimaryNodeID   domain.NodeID
	LeaseID         domain.LeaseID
	ChunkIndex      int64
	ChunkOffset     int64
	ChunkSize       int64
	Version         uint64
}

func (s *State) PrepareWrite(path string, offset, length int64, leaseID domain.LeaseID, newChunkID domain.ChunkID) (PrepareWriteResult, error) {
	fp, err := normalizePath(path)
	if err != nil {
		return PrepareWriteResult{}, err
	}
	if length <= 0 {
		return PrepareWriteResult{}, fmt.Errorf("invalid length")
	}
	fr, ok := s.Files[fp]
	if !ok {
		return PrepareWriteResult{}, domain.ErrNotFound
	}
	idx := offset / s.ChunkSize
	chunkOff := offset % s.ChunkSize
	if chunkOff+length > s.ChunkSize {
		return PrepareWriteResult{}, fmt.Errorf("write crosses chunk boundary")
	}
	ensure := int(idx) + 1
	for len(fr.Chunks) < ensure {
		fr.Chunks = append(fr.Chunks, "")
	}
	cid := fr.Chunks[idx]
	now := time.Now().UTC()

	if cid == "" {
		nodes, err := s.pickNodes(s.ReplicationFactor)
		if err != nil {
			return PrepareWriteResult{}, err
		}
		s.reserveChunkOnNodes(nodes)
		cid = newChunkID
		exp := now.Add(s.LeaseDur)
		replicas := make([]domain.ChunkReplica, len(nodes))
		for i := range nodes {
			replicas[i] = domain.ChunkReplica{NodeID: nodes[i].ID, Address: nodes[i].GRPCAddress}
		}
		s.Chunks[cid] = &chunkRec{
			ID:       cid,
			Replicas: replicas,
			Version:  1,
			LeaseID:  leaseID,
			LeaseExp: exp,
		}
		fr.Chunks[idx] = cid
		fr.Modified = now
		var sec []string
		for i := 1; i < len(replicas); i++ {
			sec = append(sec, replicas[i].Address)
		}
		return PrepareWriteResult{
			ChunkID:        cid,
			PrimaryAddr:    replicas[0].Address,
			SecondaryAddrs: sec,
			PrimaryNodeID:  replicas[0].NodeID,
			LeaseID:        leaseID,
			ChunkIndex:     idx,
			ChunkOffset:    chunkOff,
			ChunkSize:      s.ChunkSize,
			Version:        1,
		}, nil
	}

	cr, ok := s.Chunks[cid]
	if !ok {
		return PrepareWriteResult{}, domain.ErrNotFound
	}
	cr.LeaseID = leaseID
	cr.LeaseExp = now.Add(s.LeaseDur)
	var sec []string
	for i := 1; i < len(cr.Replicas); i++ {
		sec = append(sec, cr.Replicas[i].Address)
	}
	fr.Modified = now
	return PrepareWriteResult{
		ChunkID:        cid,
		PrimaryAddr:    cr.Replicas[0].Address,
		SecondaryAddrs: sec,
		PrimaryNodeID:  cr.Replicas[0].NodeID,
		LeaseID:        leaseID,
		ChunkIndex:     idx,
		ChunkOffset:    chunkOff,
		ChunkSize:      s.ChunkSize,
		Version:        cr.Version,
	}, nil
}

func (s *State) CommitChunk(path string, chunkID domain.ChunkID, chunkIndex, chunkOffset, written int64, checksum []byte, version uint64) error {
	fp, err := normalizePath(path)
	if err != nil {
		return err
	}
	fr, ok := s.Files[fp]
	if !ok {
		return domain.ErrNotFound
	}
	if int(chunkIndex) >= len(fr.Chunks) || fr.Chunks[chunkIndex] != chunkID {
		return domain.ErrChunkMismatch
	}
	cr, ok := s.Chunks[chunkID]
	if !ok {
		return domain.ErrNotFound
	}
	if version != 0 && cr.Version != version {
		return domain.ErrChunkMismatch
	}
	cr.Version++
	cr.Checksum = append([]byte(nil), checksum...)

	end := chunkIndex*s.ChunkSize + chunkOffset + written
	if end > fr.Size {
		fr.Size = end
	}
	fr.Modified = time.Now().UTC()
	return nil
}

func (s *State) GetChunkForRead(path string, offset int64) (
	chunkID domain.ChunkID,
	replicaLocs []domain.ChunkReplica,
	chunkOff int64,
	available int64,
	version uint64,
	checksum []byte,
	err error,
) {
	fp, err := normalizePath(path)
	if err != nil {
		return "", nil, 0, 0, 0, nil, err
	}
	fr, ok := s.Files[fp]
	if !ok {
		return "", nil, 0, 0, 0, nil, domain.ErrNotFound
	}
	if offset < 0 || offset >= fr.Size {
		return "", nil, 0, 0, 0, nil, fmt.Errorf("offset out of range")
	}
	idx := offset / s.ChunkSize
	chunkOff = offset % s.ChunkSize
	if int(idx) >= len(fr.Chunks) {
		return "", nil, 0, 0, 0, nil, domain.ErrNotFound
	}
	cid := fr.Chunks[idx]
	if cid == "" {
		return "", nil, 0, 0, 0, nil, domain.ErrNotFound
	}
	cr, ok := s.Chunks[cid]
	if !ok {
		return "", nil, 0, 0, 0, nil, domain.ErrNotFound
	}
	chunkStart := idx * s.ChunkSize
	bytesInFileFromChunk := fr.Size - chunkStart
	if bytesInFileFromChunk > s.ChunkSize {
		bytesInFileFromChunk = s.ChunkSize
	}
	avail := bytesInFileFromChunk - chunkOff
	if avail < 0 {
		avail = 0
	}
	var sum []byte
	if len(cr.Checksum) > 0 {
		sum = append([]byte(nil), cr.Checksum...)
	}
	return cid, append([]domain.ChunkReplica(nil), cr.Replicas...), chunkOff, avail, cr.Version, sum, nil
}

func (s *State) Delete(path string) ([]domain.ChunkDeleteInfo, error) {
	// Mirror metadata.Store: normalize, decide file vs dir, delete recursively.
	dir, derr := normalizeDir(path)
	if derr == nil {
		if _, ok := s.Dirs[dir]; ok && dir != "/" {
			return s.deleteDir(dir)
		}
	}
	fp, err := normalizePath(path)
	if err != nil {
		return nil, err
	}
	return s.deleteFile(fp)
}

func (s *State) deleteFile(fp string) ([]domain.ChunkDeleteInfo, error) {
	fr, ok := s.Files[fp]
	if !ok {
		return nil, domain.ErrNotFound
	}
	delete(s.Files, fp)
	var infos []domain.ChunkDeleteInfo
	for _, cid := range fr.Chunks {
		if cid == "" {
			continue
		}
		cr, ok := s.Chunks[cid]
		if !ok {
			continue
		}
		var addrs []string
		for _, r := range cr.Replicas {
			addrs = append(addrs, r.Address)
		}
		infos = append(infos, domain.ChunkDeleteInfo{ChunkID: cid, Replicas: addrs})
		s.releaseChunkFromReplicas(cr.Replicas)
		delete(s.Chunks, cid)
	}
	return infos, nil
}

func (s *State) deleteDir(d string) ([]domain.ChunkDeleteInfo, error) {
	// Refuse if directory doesn't exist.
	if _, ok := s.Dirs[d]; !ok {
		return nil, domain.ErrNotFound
	}
	prefix := d
	if prefix != "/" {
		prefix += "/"
	}
	// Collect nested files first.
	var toDeleteFiles []string
	for fp := range s.Files {
		if strings.HasPrefix(fp, prefix) {
			toDeleteFiles = append(toDeleteFiles, fp)
		}
	}
	var infos []domain.ChunkDeleteInfo
	for _, fp := range toDeleteFiles {
		fi, err := s.deleteFile(fp)
		if err != nil && err != domain.ErrNotFound {
			return nil, err
		}
		infos = append(infos, fi...)
	}
	// Delete nested dirs.
	for dd := range s.Dirs {
		if dd == "/" {
			continue
		}
		if dd == d || strings.HasPrefix(dd, prefix) {
			delete(s.Dirs, dd)
		}
	}
	return infos, nil
}

