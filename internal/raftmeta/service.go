package raftmeta

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"

	"godfs/internal/domain"
)

// Service is a Raft-backed implementation of the MasterStore (metadata operations).
type Service struct {
	raft *raft.Raft
	fsm  *FSM

	leaderGrpcByRaftAddr map[string]string // raftAddr -> grpcAddr (best-effort redirect)
}

func NewService(r *raft.Raft, fsm *FSM, leaderGrpcByRaftAddr map[string]string) *Service {
	return &Service{
		raft:                r,
		fsm:                 fsm,
		leaderGrpcByRaftAddr: leaderGrpcByRaftAddr,
	}
}

func (s *Service) IsLeader() bool {
	return s.raft.State() == raft.Leader
}

func (s *Service) LeaderGRPCAddr() string {
	ldr := string(s.raft.Leader())
	if ldr == "" {
		return ""
	}
	if s.leaderGrpcByRaftAddr == nil {
		return ""
	}
	if v, ok := s.leaderGrpcByRaftAddr[ldr]; ok {
		return v
	}
	return ""
}

func (s *Service) apply(ctx context.Context, b []byte) (any, error) {
	f := s.raft.Apply(b, 10*time.Second)
	errCh := make(chan error, 1)
	go func() { errCh <- f.Error() }()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-errCh:
		if err != nil {
			return nil, err
		}
	}
	if err := f.Error(); err != nil { // already done, should be nil
		return nil, err
	}
	resp := f.Response()
	if err, ok := resp.(error); ok && err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *Service) RegisterNode(ctx context.Context, n domain.ChunkNode) error {
	b, err := encodeCommand(cmdRegisterNode, n)
	if err != nil {
		return err
	}
	_, err = s.apply(ctx, b)
	return err
}

func (s *Service) Mkdir(ctx context.Context, p string) error {
	b, err := encodeCommand(cmdMkdir, struct{ Path string }{Path: p})
	if err != nil {
		return err
	}
	_, err = s.apply(ctx, b)
	return err
}

func (s *Service) CreateFile(ctx context.Context, p string) (domain.FileID, error) {
	id := domain.FileID(uuid.NewString())
	at := time.Now().UTC().Unix()
	b, err := encodeCommand(cmdCreateFile, struct {
		Path   string
		FileID domain.FileID
		AtUnix int64
	}{Path: p, FileID: id, AtUnix: at})
	if err != nil {
		return "", err
	}
	_, err = s.apply(ctx, b)
	if err != nil {
		return "", err
	}
	return id, nil
}

func (s *Service) Rename(ctx context.Context, oldPath, newPath string) error {
	b, err := encodeCommand(cmdRename, struct {
		Old string
		New string
	}{Old: oldPath, New: newPath})
	if err != nil {
		return err
	}
	_, err = s.apply(ctx, b)
	return err
}

func (s *Service) Delete(ctx context.Context, p string) ([]domain.ChunkDeleteInfo, error) {
	b, err := encodeCommand(cmdDelete, struct{ Path string }{Path: p})
	if err != nil {
		return nil, err
	}
	resp, err := s.apply(ctx, b)
	if err != nil {
		return nil, err
	}
	infos, ok := resp.([]domain.ChunkDeleteInfo)
	if !ok {
		return nil, fmt.Errorf("unexpected delete response: %T", resp)
	}
	return infos, nil
}

func (s *Service) Stat(ctx context.Context, p string) (bool, int64, time.Time, time.Time, uint32, error) {
	_ = ctx
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	return s.fsm.st.Stat(p)
}

func (s *Service) ListDir(ctx context.Context, p string) ([]string, bool, error) {
	_ = ctx
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	return s.fsm.st.ListDir(p)
}

func (s *Service) PrepareWrite(ctx context.Context, p string, offset, length int64) (
	domain.ChunkID, string, []string, domain.NodeID, domain.LeaseID, int64, int64, int64, uint64, error,
) {
	leaseID := domain.LeaseID(uuid.NewString())
	newChunkID := domain.ChunkID(uuid.NewString())
	at := time.Now().UTC().Unix()
	b, err := encodeCommand(cmdPrepareWrite, struct {
		Path       string
		Offset     int64
		Length     int64
		LeaseID    domain.LeaseID
		NewChunkID domain.ChunkID
		AtUnix     int64
	}{Path: p, Offset: offset, Length: length, LeaseID: leaseID, NewChunkID: newChunkID, AtUnix: at})
	if err != nil {
		return "", "", nil, "", "", 0, 0, 0, 0, err
	}
	resp, err := s.apply(ctx, b)
	if err != nil {
		return "", "", nil, "", "", 0, 0, 0, 0, err
	}
	res, ok := resp.(PrepareWriteResult)
	if !ok {
		return "", "", nil, "", "", 0, 0, 0, 0, fmt.Errorf("unexpected prepare write response: %T", resp)
	}
	return res.ChunkID, res.PrimaryAddr, res.SecondaryAddrs, res.PrimaryNodeID, res.LeaseID, res.ChunkIndex, res.ChunkOffset, res.ChunkSize, res.Version, nil
}

func (s *Service) CommitChunk(ctx context.Context, p string, chunkID domain.ChunkID, chunkIndex, chunkOffset, written int64, checksum []byte, version uint64) error {
	at := time.Now().UTC().Unix()
	b, err := encodeCommand(cmdCommitChunk, struct {
		Path        string
		ChunkID     domain.ChunkID
		ChunkIndex  int64
		ChunkOffset int64
		Written     int64
		Checksum    []byte
		Version     uint64
		AtUnix      int64
	}{Path: p, ChunkID: chunkID, ChunkIndex: chunkIndex, ChunkOffset: chunkOffset, Written: written, Checksum: checksum, Version: version, AtUnix: at})
	if err != nil {
		return err
	}
	_, err = s.apply(ctx, b)
	return err
}

func (s *Service) GetChunkForRead(ctx context.Context, p string, offset int64) (domain.ChunkID, []domain.ChunkReplica, int64, int64, uint64, []byte, error) {
	_ = ctx
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	cid, reps, off, avail, ver, sum, err := s.fsm.st.GetChunkForRead(p, offset)
	if err != nil {
		return "", nil, 0, 0, 0, nil, err
	}
	// Prefer alive replicas first (best effort; uses local clock).
	if s.fsm.st.NodeDeadAfter > 0 && len(reps) > 1 {
		at := time.Now().UTC()
		alive := make([]domain.ChunkReplica, 0, len(reps))
		dead := make([]domain.ChunkReplica, 0, len(reps))
		for _, r := range reps {
			if s.fsm.st.isAliveAt(r.NodeID, at) {
				alive = append(alive, r)
			} else {
				dead = append(dead, r)
			}
		}
		reps = append(alive, dead...)
	}
	return cid, reps, off, avail, ver, sum, nil
}

func (s *Service) Heartbeat(ctx context.Context, nodeID domain.NodeID, capacityBytes, usedBytes int64) error {
	at := time.Now().UTC().Unix()
	b, err := encodeCommand(cmdHeartbeat, struct {
		NodeID        domain.NodeID
		CapacityBytes int64
		UsedBytes     int64
		AtUnix        int64
	}{NodeID: nodeID, CapacityBytes: capacityBytes, UsedBytes: usedBytes, AtUnix: at})
	if err != nil {
		return err
	}
	_, err = s.apply(ctx, b)
	return err
}

func (s *Service) AddReplica(ctx context.Context, chunkID domain.ChunkID, nodeID domain.NodeID, addr string) error {
	at := time.Now().UTC().Unix()
	b, err := encodeCommand(cmdAddReplica, struct {
		ChunkID domain.ChunkID
		NodeID  domain.NodeID
		Address string
		AtUnix  int64
	}{ChunkID: chunkID, NodeID: nodeID, Address: addr, AtUnix: at})
	if err != nil {
		return err
	}
	_, err = s.apply(ctx, b)
	return err
}

func (s *Service) ClearPendingDeleteAddr(ctx context.Context, chunkID domain.ChunkID, addr string) error {
	b, err := encodeCommand(cmdClearPendingDeleteAddr, struct {
		ChunkID domain.ChunkID
		Addr    string
	}{ChunkID: chunkID, Addr: addr})
	if err != nil {
		return err
	}
	_, err = s.apply(ctx, b)
	return err
}

func (s *Service) MarkPendingDeleteAttempt(ctx context.Context, chunkID domain.ChunkID, addr string, attempts int, nextAttemptUnix int64) error {
	b, err := encodeCommand(cmdMarkPendingDeleteAttempt, struct {
		ChunkID         domain.ChunkID
		Addr            string
		Attempts        int
		NextAttemptUnix int64
	}{ChunkID: chunkID, Addr: addr, Attempts: attempts, NextAttemptUnix: nextAttemptUnix})
	if err != nil {
		return err
	}
	_, err = s.apply(ctx, b)
	return err
}

func (s *Service) SnapshotNodes() []domain.ChunkNode {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	return append([]domain.ChunkNode(nil), s.fsm.st.Nodes...)
}

func (s *Service) SnapshotChunkIDs() map[domain.ChunkID]struct{} {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	out := make(map[domain.ChunkID]struct{}, len(s.fsm.st.Chunks))
	for cid := range s.fsm.st.Chunks {
		out[cid] = struct{}{}
	}
	return out
}

var ErrNotLeader = errors.New("not raft leader")

// ParsePeers parses a comma-separated list of peers.
// Supported formats:
// - nodeID@raftAddr
// - nodeID@raftAddr@grpcAddr (recommended for redirects)
func ParsePeers(s string) (map[string]raft.ServerAddress, map[string]string, error) {
	peers := map[string]raft.ServerAddress{}
	grpcByRaft := map[string]string{}
	if strings.TrimSpace(s) == "" {
		return peers, grpcByRaft, nil
	}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		fields := strings.Split(part, "@")
		if len(fields) < 2 || len(fields) > 3 {
			return nil, nil, fmt.Errorf("invalid peer %q", part)
		}
		nodeID := fields[0]
		raftAddr := fields[1]
		peers[nodeID] = raft.ServerAddress(raftAddr)
		if len(fields) == 3 {
			grpcByRaft[raftAddr] = fields[2]
		}
	}
	return peers, grpcByRaft, nil
}

