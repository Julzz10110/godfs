package grpc

import (
	"context"
	"errors"
	"path"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	godfsv1 "godfs/api/proto/godfs/v1"
	"godfs/internal/adapter/repository/metadata"
	"godfs/internal/domain"
)

// MasterServer implements godfsv1.MasterServiceServer.
type MasterServer struct {
	godfsv1.UnimplementedMasterServiceServer
	Store *metadata.Store
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
	case errors.Is(err, domain.ErrParentNotFound):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, domain.ErrChunkMismatch):
		return status.Error(codes.Aborted, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func (m *MasterServer) RegisterNode(ctx context.Context, req *godfsv1.RegisterNodeRequest) (*godfsv1.RegisterNodeResponse, error) {
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
	id, err := m.Store.CreateFile(ctx, req.Path)
	if err != nil {
		return nil, mapErr(err)
	}
	return &godfsv1.CreateFileResponse{FileId: string(id)}, nil
}

func (m *MasterServer) Mkdir(ctx context.Context, req *godfsv1.MkdirRequest) (*godfsv1.MkdirResponse, error) {
	if err := m.Store.Mkdir(ctx, req.Path); err != nil {
		return nil, mapErr(err)
	}
	return &godfsv1.MkdirResponse{}, nil
}

func (m *MasterServer) Delete(ctx context.Context, req *godfsv1.DeleteRequest) (*godfsv1.DeleteResponse, error) {
	if _, err := m.Store.Delete(ctx, req.Path); err != nil {
		return nil, mapErr(err)
	}
	return &godfsv1.DeleteResponse{}, nil
}

func (m *MasterServer) Rename(ctx context.Context, req *godfsv1.RenameRequest) (*godfsv1.RenameResponse, error) {
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
	cid, addr, lease, idx, off, csize, ver, err := m.Store.PrepareWrite(ctx, req.Path, req.Offset, req.Length)
	if err != nil {
		return nil, mapErr(err)
	}
	return &godfsv1.PrepareWriteResponse{
		ChunkId:         string(cid),
		PrimaryAddress:  addr,
		LeaseId:         string(lease),
		ChunkIndex:      idx,
		ChunkOffset:     off,
		ChunkSize:       csize,
		Version:         ver,
	}, nil
}

func (m *MasterServer) CommitChunk(ctx context.Context, req *godfsv1.CommitChunkRequest) (*godfsv1.CommitChunkResponse, error) {
	err := m.Store.CommitChunk(ctx, req.Path, domain.ChunkID(req.ChunkId), req.ChunkIndex, req.ChunkOffset, req.Written, req.ChecksumSha256, req.Version)
	if err != nil {
		return nil, mapErr(err)
	}
	return &godfsv1.CommitChunkResponse{}, nil
}

func (m *MasterServer) GetChunkForRead(ctx context.Context, req *godfsv1.GetChunkForReadRequest) (*godfsv1.GetChunkForReadResponse, error) {
	cid, reps, off, avail, ver, err := m.Store.GetChunkForRead(ctx, req.Path, req.Offset)
	if err != nil {
		return nil, mapErr(err)
	}
	return &godfsv1.GetChunkForReadResponse{
		ChunkId:           string(cid),
		ReplicaAddresses:  reps,
		ChunkOffset:       off,
		AvailableInChunk:  avail,
		Version:           ver,
	}, nil
}
