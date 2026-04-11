package grpc

import (
	"context"
	"errors"
	"path"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	godfsv1 "godfs/api/proto/godfs/v1"
	"godfs/internal/domain"
	"godfs/internal/security"
	"godfs/internal/usecase"
)

// MasterServer implements godfsv1.MasterServiceServer.
type MasterServer struct {
	godfsv1.UnimplementedMasterServiceServer
	Store usecase.MasterStore
}

type leaderAware interface {
	IsLeader() bool
	LeaderGRPCAddr() string
}

func (m *MasterServer) ensureLeader() error {
	la, ok := m.Store.(leaderAware)
	if !ok {
		return nil // single-master mode
	}
	if la.IsLeader() {
		return nil
	}
	ldr := la.LeaderGRPCAddr()
	if ldr != "" {
		return status.Errorf(codes.FailedPrecondition, "not leader (leader_grpc=%s)", ldr)
	}
	return status.Error(codes.FailedPrecondition, "not leader")
}

func mapErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, domain.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, domain.ErrAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, domain.ErrNotEmpty):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, domain.ErrIsDir):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, domain.ErrNotDir):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, domain.ErrInvalidPath):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, domain.ErrNoChunkServer):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, domain.ErrInsufficientChunkServers):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, domain.ErrParentNotFound):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, domain.ErrChunkMismatch):
		return status.Error(codes.Aborted, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func (m *MasterServer) RegisterNode(ctx context.Context, req *godfsv1.RegisterNodeRequest) (*godfsv1.RegisterNodeResponse, error) {
	if err := m.ensureLeader(); err != nil {
		return nil, err
	}
	err := m.Store.RegisterNode(ctx, domain.ChunkNode{
		ID:            domain.NodeID(req.NodeId),
		GRPCAddress:   req.GrpcAddress,
		CapacityBytes: req.CapacityBytes,
	})
	if err != nil {
		return nil, mapErr(err)
	}
	return &godfsv1.RegisterNodeResponse{}, nil
}

func (m *MasterServer) CreateFile(ctx context.Context, req *godfsv1.CreateFileRequest) (*godfsv1.CreateFileResponse, error) {
	if err := m.ensureLeader(); err != nil {
		return nil, err
	}
	id, err := m.Store.CreateFile(ctx, req.Path)
	if err != nil {
		return nil, mapErr(err)
	}
	return &godfsv1.CreateFileResponse{FileId: string(id)}, nil
}

func (m *MasterServer) Mkdir(ctx context.Context, req *godfsv1.MkdirRequest) (*godfsv1.MkdirResponse, error) {
	if err := m.ensureLeader(); err != nil {
		return nil, err
	}
	if err := m.Store.Mkdir(ctx, req.Path); err != nil {
		return nil, mapErr(err)
	}
	return &godfsv1.MkdirResponse{}, nil
}

func (m *MasterServer) Delete(ctx context.Context, req *godfsv1.DeleteRequest) (*godfsv1.DeleteResponse, error) {
	if err := m.ensureLeader(); err != nil {
		return nil, err
	}
	chunks, err := m.Store.Delete(ctx, req.Path)
	if err != nil {
		return nil, mapErr(err)
	}
	for _, ch := range chunks {
		for _, addr := range ch.Replicas {
			if err := deleteChunkOnPeer(ctx, addr, string(ch.ChunkID)); err != nil {
				return nil, status.Errorf(codes.Internal, "delete chunk %s on %s: %v", ch.ChunkID, addr, err)
			}
		}
	}
	return &godfsv1.DeleteResponse{}, nil
}

func deleteChunkOnPeer(ctx context.Context, addr, chunkID string) error {
	var last error
	for attempt := range 4 {
		if attempt > 0 {
			t := time.NewTimer(time.Duration(50*attempt) * time.Millisecond)
			select {
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			case <-t.C:
			}
		}
		last = deleteChunkOnce(ctx, addr, chunkID)
		if last == nil {
			return nil
		}
	}
	return last
}

func deleteChunkOnce(ctx context.Context, addr, chunkID string) error {
	dopts, err := security.ClientDialOptions()
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient(addr, dopts...)
	if err != nil {
		return err
	}
	defer conn.Close()
	cli := godfsv1.NewChunkServiceClient(conn)
	_, err = cli.DeleteChunk(ctx, &godfsv1.DeleteChunkRequest{ChunkId: chunkID})
	return err
}

func (m *MasterServer) Rename(ctx context.Context, req *godfsv1.RenameRequest) (*godfsv1.RenameResponse, error) {
	if err := m.ensureLeader(); err != nil {
		return nil, err
	}
	if err := m.Store.Rename(ctx, req.OldPath, req.NewPath); err != nil {
		return nil, mapErr(err)
	}
	return &godfsv1.RenameResponse{}, nil
}

func (m *MasterServer) Stat(ctx context.Context, req *godfsv1.StatRequest) (*godfsv1.StatResponse, error) {
	isDir, sz, cr, mod, mode, err := m.Store.Stat(ctx, req.Path)
	if err != nil {
		return nil, mapErr(err)
	}
	return &godfsv1.StatResponse{
		IsDir:           isDir,
		Size:            sz,
		CreatedAtUnix:   cr.Unix(),
		ModifiedAtUnix:  mod.Unix(),
		Mode:            mode,
	}, nil
}

func (m *MasterServer) ListDir(ctx context.Context, req *godfsv1.ListDirRequest) (*godfsv1.ListDirResponse, error) {
	names, _, err := m.Store.ListDir(ctx, req.Path)
	if err != nil {
		return nil, mapErr(err)
	}
	var entries []*godfsv1.DirEntry
	for _, n := range names {
		full := path.Join(req.Path, n)
		isDir, sz, _, _, _, err := m.Store.Stat(ctx, full)
		if err != nil {
			continue
		}
		entries = append(entries, &godfsv1.DirEntry{Name: n, IsDir: isDir, Size: sz})
	}
	return &godfsv1.ListDirResponse{Entries: entries}, nil
}

func (m *MasterServer) PrepareWrite(ctx context.Context, req *godfsv1.PrepareWriteRequest) (*godfsv1.PrepareWriteResponse, error) {
	if err := m.ensureLeader(); err != nil {
		return nil, err
	}
	cid, addr, sec, primaryID, lease, idx, off, csize, ver, err := m.Store.PrepareWrite(ctx, req.Path, req.Offset, req.Length)
	if err != nil {
		return nil, mapErr(err)
	}
	return &godfsv1.PrepareWriteResponse{
		ChunkId:             string(cid),
		PrimaryAddress:      addr,
		SecondaryAddresses:  sec,
		PrimaryNodeId:       string(primaryID),
		LeaseId:             string(lease),
		ChunkIndex:          idx,
		ChunkOffset:         off,
		ChunkSize:           csize,
		Version:             ver,
	}, nil
}

func (m *MasterServer) CommitChunk(ctx context.Context, req *godfsv1.CommitChunkRequest) (*godfsv1.CommitChunkResponse, error) {
	if err := m.ensureLeader(); err != nil {
		return nil, err
	}
	err := m.Store.CommitChunk(ctx, req.Path, domain.ChunkID(req.ChunkId), req.ChunkIndex, req.ChunkOffset, req.Written, req.ChecksumSha256, req.Version)
	if err != nil {
		return nil, mapErr(err)
	}
	return &godfsv1.CommitChunkResponse{}, nil
}

func (m *MasterServer) GetChunkForRead(ctx context.Context, req *godfsv1.GetChunkForReadRequest) (*godfsv1.GetChunkForReadResponse, error) {
	cid, locs, off, avail, ver, cksum, err := m.Store.GetChunkForRead(ctx, req.Path, req.Offset)
	if err != nil {
		return nil, mapErr(err)
	}
	addrs := make([]string, len(locs))
	protoLocs := make([]*godfsv1.ReplicaLocation, len(locs))
	for i, r := range locs {
		addrs[i] = r.Address
		protoLocs[i] = &godfsv1.ReplicaLocation{
			NodeId:       string(r.NodeID),
			GrpcAddress:  r.Address,
		}
	}
	return &godfsv1.GetChunkForReadResponse{
		ChunkId:               string(cid),
		ReplicaAddresses:      addrs,
		ReplicaLocations:      protoLocs,
		ChunkOffset:           off,
		AvailableInChunk:      avail,
		Version:               ver,
		ChunkChecksumSha256:   cksum,
	}, nil
}

func (m *MasterServer) Heartbeat(ctx context.Context, req *godfsv1.HeartbeatRequest) (*godfsv1.HeartbeatResponse, error) {
	if err := m.ensureLeader(); err != nil {
		return nil, err
	}
	if err := m.Store.Heartbeat(ctx, domain.NodeID(req.NodeId), req.CapacityBytes, req.UsedBytes); err != nil {
		return nil, mapErr(err)
	}
	return &godfsv1.HeartbeatResponse{ServerTimeUnix: time.Now().UTC().Unix()}, nil
}
