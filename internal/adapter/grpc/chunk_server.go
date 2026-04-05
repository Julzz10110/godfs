package grpc

import (
	"context"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	godfsv1 "godfs/api/proto/godfs/v1"
	chstor "godfs/internal/adapter/repository/chunk"
)

// ChunkServer implements godfsv1.ChunkServiceServer.
type ChunkServer struct {
	godfsv1.UnimplementedChunkServiceServer
	Store *chstor.FSStore
}

func (c *ChunkServer) WriteChunk(stream godfsv1.ChunkService_WriteChunkServer) error {
	var meta *godfsv1.WriteChunkMeta
	var buf []byte

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if m := req.GetMeta(); m != nil {
			meta = m
		}
		if d := req.GetData(); d != nil {
			buf = append(buf, d...)
		}
	}
	if meta == nil {
		return status.Error(codes.InvalidArgument, "missing meta")
	}

	n, sum, err := c.Store.WriteAt(meta.ChunkId, meta.OffsetInChunk, buf)
	if err != nil {
		return status.Errorf(codes.Internal, "write: %v", err)
	}

	return stream.SendAndClose(&godfsv1.WriteChunkResponse{
		BytesWritten:   n,
		ChecksumSha256: sum,
	})
}

func (c *ChunkServer) ReadChunk(req *godfsv1.ReadChunkRequest, stream godfsv1.ChunkService_ReadChunkServer) error {
	buf := make([]byte, 32*1024)
	remain := req.Length
	off := req.OffsetInChunk

	for remain > 0 {
		n := int64(len(buf))
		if n > remain {
			n = remain
		}
		rn, err := c.Store.ReadAt(req.ChunkId, off, buf[:n])
		if err != nil {
			return status.Errorf(codes.Internal, "read: %v", err)
		}
		if rn == 0 {
			break
		}
		if err := stream.Send(&godfsv1.ReadChunkResponse{Data: buf[:rn]}); err != nil {
			return err
		}
		off += int64(rn)
		remain -= int64(rn)
	}
	return nil
}

func (c *ChunkServer) DeleteChunk(_ context.Context, req *godfsv1.DeleteChunkRequest) (*godfsv1.DeleteChunkResponse, error) {
	if err := c.Store.Delete(req.ChunkId); err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &godfsv1.DeleteChunkResponse{}, nil
}
