package grpc

import (
	"context"
	"os"
	"strconv"

	"google.golang.org/grpc"

	godfsv1 "godfs/api/proto/godfs/v1"
	"godfs/internal/security"
)

func syncChunkPartBytes() int {
	const def = 256 * 1024
	v := os.Getenv("GODFS_SYNC_CHUNK_PART_BYTES")
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 4096 {
		return def
	}
	return n
}

// ReplicateFullChunk pushes full chunk bytes from primary to a secondary via SyncChunk.
func ReplicateFullChunk(ctx context.Context, targetAddr, chunkID string, data []byte) error {
	dopts, err := security.ClientDialOptions()
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient(targetAddr, dopts...)
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
	part := syncChunkPartBytes()
	for i := 0; i < len(data); i += part {
		end := i + part
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
