package grpc

import (
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	godfsv1 "godfs/api/proto/godfs/v1"
)

const pullReadMax = 64 * 1024 * 1024

// PullChunk streams a chunk from source_peer_address and stores it locally (pull replication path).
func (c *ChunkServer) PullChunk(req *godfsv1.PullChunkRequest, stream godfsv1.ChunkService_PullChunkServer) error {
	if req.ChunkId == "" || req.SourcePeerAddress == "" {
		return status.Error(codes.InvalidArgument, "chunk_id and source_peer_address required")
	}
	ctx := stream.Context()

	conn, err := grpc.NewClient(req.SourcePeerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
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

	var buf []byte
	for {
		msg, err := rc.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "recv: %v", err)
		}
		buf = append(buf, msg.Data...)
		if err := stream.Send(&godfsv1.PullChunkResponse{Data: msg.Data}); err != nil {
			return err
		}
	}
	if err := c.Store.WriteFull(req.ChunkId, buf); err != nil {
		return status.Errorf(codes.Internal, "local write: %v", err)
	}
	return nil
}
