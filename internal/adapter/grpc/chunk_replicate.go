package grpc

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	godfsv1 "godfs/api/proto/godfs/v1"
)

const syncChunkPart = 256 * 1024

// ReplicateFullChunk pushes full chunk bytes from primary to a secondary via SyncChunk.
func ReplicateFullChunk(ctx context.Context, targetAddr, chunkID string, data []byte) error {
	conn, err := grpc.NewClient(targetAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	cli := godfsv1.NewChunkServiceClient(conn)
	stream, err := cli.SyncChunk(ctx)
	if err != nil {
		return err
	}
	if err := stream.Send(&godfsv1.SyncChunkRequest{
		Frame: &godfsv1.SyncChunkRequest_Meta{
			Meta: &godfsv1.SyncChunkMeta{ChunkId: chunkID},
		},
	}); err != nil {
		return err
	}
	for i := 0; i < len(data); i += syncChunkPart {
		end := i + syncChunkPart
		if end > len(data) {
			end = len(data)
		}
		if err := stream.Send(&godfsv1.SyncChunkRequest{
			Frame: &godfsv1.SyncChunkRequest_Data{Data: data[i:end]},
		}); err != nil {
			return err
		}
	}
	_, err = stream.CloseAndRecv()
	return err
}
