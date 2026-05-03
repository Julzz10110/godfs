package grpc

import (
	"context"
	"errors"
	"io"
	"os"
	"strconv"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	godfsv1 "godfs/api/proto/godfs/v1"
	chstor "godfs/internal/adapter/repository/chunk"
	"godfs/internal/domain"
)

func chunkStoreStatus(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, domain.ErrInvalidPath) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return status.Errorf(codes.Internal, "%v", err)
}

func readChunkFrameBytes() int {
	const def = 32 * 1024
	v := os.Getenv("GODFS_CHUNK_READ_FRAME_BYTES")
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1024 {
		return def
	}
	return n
}

// ChunkServer implements godfsv1.ChunkServiceServer.
type ChunkServer struct {
	godfsv1.UnimplementedChunkServiceServer
	Store     *chstor.FSStore
	ReadCache *chstor.ReadRangeCache // optional hot read cache
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
		return chunkStoreStatus(err)
	}

	ctx := stream.Context()
	if len(meta.SecondaryAddresses) > 0 {
		full, rerr := c.Store.ReadAll(meta.ChunkId)
		if rerr != nil {
			return chunkStoreStatus(rerr)
		}
		g, gctx := errgroup.WithContext(ctx)
		for _, peer := range meta.SecondaryAddresses {
			g.Go(func() error {
				return ReplicateFullChunk(gctx, peer, meta.ChunkId, full)
			})
		}
		if err := g.Wait(); err != nil {
			return status.Errorf(codes.Internal, "replicate: %v", err)
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
		return chunkStoreStatus(err)
	}
	return stream.SendAndClose(&godfsv1.SyncChunkResponse{BytesCopied: int64(len(buf))})
}

func (c *ChunkServer) ReadChunk(req *godfsv1.ReadChunkRequest, stream godfsv1.ChunkService_ReadChunkServer) error {
	if c.ReadCache != nil && req.Length > 0 {
		if data, ok := c.ReadCache.Get(req.ChunkId, req.OffsetInChunk, req.Length); ok {
			return sendReadChunks(stream, data)
		}
	}

	buf := make([]byte, readChunkFrameBytes())
	remain := req.Length
	off := req.OffsetInChunk
	var acc []byte
	cacheFull := c.ReadCache != nil && req.Length > 0 && req.Length <= c.ReadCache.MaxEntryBytes()

	if cacheFull {
		acc = make([]byte, 0, req.Length)
	}

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
		if cacheFull {
			acc = append(acc, buf[:rn]...)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return chunkStoreStatus(err)
		}
		off += int64(rn)
		remain -= int64(rn)
	}
	if cacheFull && int64(len(acc)) == req.Length {
		c.ReadCache.Add(req.ChunkId, req.OffsetInChunk, req.Length, acc)
	}
	return nil
}

func sendReadChunks(stream godfsv1.ChunkService_ReadChunkServer, data []byte) error {
	chunk := readChunkFrameBytes()
	for len(data) > 0 {
		n := chunk
		if n > len(data) {
			n = len(data)
		}
		if err := stream.Send(&godfsv1.ReadChunkResponse{Data: data[:n]}); err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

func (c *ChunkServer) DeleteChunk(_ context.Context, req *godfsv1.DeleteChunkRequest) (*godfsv1.DeleteChunkResponse, error) {
	if err := c.Store.Delete(req.ChunkId); err != nil {
		return nil, chunkStoreStatus(err)
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
		return nil, chunkStoreStatus(err)
	}
	return &godfsv1.ChecksumChunkResponse{ChecksumSha256: sum, SizeBytes: sz, ModifiedAtUnix: mt.Unix()}, nil
}
