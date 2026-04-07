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

	ctx := stream.Context()
	if len(meta.SecondaryAddresses) > 0 {
		full, rerr := c.Store.ReadAll(meta.ChunkId)
		if rerr != nil {
			return status.Errorf(codes.Internal, "read for replicate: %v", rerr)
		}
		for _, peer := range meta.SecondaryAddresses {
			if err := ReplicateFullChunk(ctx, peer, meta.ChunkId, full); err != nil {
				return status.Errorf(codes.Internal, "replicate to %s: %v", peer, err)
			}
		}
	}

	return stream.SendAndClose(&godfsv1.WriteChunkResponse{
		BytesWritten:   n,
		ChecksumSha256: sum,
	})
}

func (c *ChunkServer) SyncChunk(stream godfsv1.ChunkService_SyncChunkServer) error {
	var chunkID string
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
			chunkID = m.ChunkId
		}
		if d := req.GetData(); d != nil {
			buf = append(buf, d...)
		}
	}
	if chunkID == "" {
		return status.Error(codes.InvalidArgument, "missing meta")
	}
	if err := c.Store.WriteFull(chunkID, buf); err != nil {
		return status.Errorf(codes.Internal, "sync write: %v", err)
	}
	return stream.SendAndClose(&godfsv1.SyncChunkResponse{BytesCopied: int64(len(buf))})
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
		if rn == 0 {
			break
		}
		if err := stream.Send(&godfsv1.ReadChunkResponse{Data: buf[:rn]}); err != nil {
			return err
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "read: %v", err)
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

func (c *ChunkServer) ListChunks(_ context.Context, _ *godfsv1.ListChunksRequest) (*godfsv1.ListChunksResponse, error) {
	m, err := c.Store.ListChunks()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list: %v", err)
	}
	out := make([]*godfsv1.ChunkInfo, 0, len(m))
	for id, mt := range m {
		out = append(out, &godfsv1.ChunkInfo{ChunkId: id, ModifiedAtUnix: mt.Unix()})
	}
	return &godfsv1.ListChunksResponse{Chunks: out}, nil
}

func (c *ChunkServer) ChecksumChunk(_ context.Context, req *godfsv1.ChecksumChunkRequest) (*godfsv1.ChecksumChunkResponse, error) {
	if req.ChunkId == "" {
		return nil, status.Error(codes.InvalidArgument, "chunk_id required")
	}
	sum, sz, mt, err := c.Store.Checksum(req.ChunkId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "checksum: %v", err)
	}
	return &godfsv1.ChecksumChunkResponse{ChecksumSha256: sum, SizeBytes: sz, ModifiedAtUnix: mt.Unix()}, nil
}
