package grpc

import (
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	godfsv1 "godfs/api/proto/godfs/v1"
	"godfs/internal/security"
)

const pullReadMax = 64 * 1024 * 1024

// PullChunk streams a chunk from source_peer_address and stores it locally (pull replication path).
func (c *ChunkServer) PullChunk(req *godfsv1.PullChunkRequest, stream godfsv1.ChunkService_PullChunkServer) error {
	if req.ChunkId == "" || req.SourcePeerAddress == "" {
		return status.Error(codes.InvalidArgument, "chunk_id and source_peer_address required")
	}
	ctx := stream.Context()

	dopts, err := security.ClientDialOptions()
	if err != nil {
		return status.Errorf(codes.Internal, "dial options: %v", err)
	}
	conn, err := grpc.NewClient(req.SourcePeerAddress, dopts...)
	if err != nil {
		return status.Errorf(codes.Unavailable, "dial source: %v", err)
	}
	defer conn.Close()

	cli := godfsv1.NewChunkServiceClient(conn)
	rc, err := cli.ReadChunk(ctx, &godfsv1.ReadChunkRequest{
		ChunkId:       req.ChunkId,
		OffsetInChunk: 0,
		Length:        pullReadMax,
	})
	if err != nil {
		return status.Errorf(codes.Internal, "read source: %v", err)
	}

	pr, pw := io.Pipe()
	writeErrCh := make(chan error, 1)
	go func() {
		_, err := c.Store.WriteFullFromReader(req.ChunkId, pr, pullReadMax)
		_ = pr.CloseWithError(err)
		writeErrCh <- err
	}()

	for {
		msg, err := rc.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			_ = pw.CloseWithError(err)
			return status.Errorf(codes.Internal, "recv: %v", err)
		}
		if len(msg.Data) > 0 {
			if _, err := pw.Write(msg.Data); err != nil {
				_ = pw.CloseWithError(err)
				return status.Errorf(codes.Internal, "local write: %v", err)
			}
		}
		if err := stream.Send(&godfsv1.PullChunkResponse{Data: msg.Data}); err != nil {
			_ = pw.CloseWithError(err)
			return err
		}
	}
	_ = pw.Close()
	if err := <-writeErrCh; err != nil {
		return status.Errorf(codes.Internal, "local write: %v", err)
	}
	return nil
}
