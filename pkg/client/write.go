package client

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	godfsv1 "godfs/api/proto/godfs/v1"
)

func (c *Client) writeParallelism() int {
	n := c.writeParallel
	if n <= 0 {
		n = 4
	}
	if v := os.Getenv("GODFS_CLIENT_WRITE_PARALLELISM"); v != "" {
		if x, err := strconv.Atoi(v); err == nil && x > 0 {
			n = x
		}
	}
	if n > 64 {
		n = 64
	}
	return n
}

type writeSeg struct {
	pos int64
	n   int64
}

func (c *Client) writeSegmentOnce(ctx context.Context, path string, data []byte, pos, n int64) error {
	pw, err := c.master.PrepareWrite(ctx, &godfsv1.PrepareWriteRequest{
		Path:   path,
		Offset: pos,
		Length: n,
	})
	if err != nil {
		return fmt.Errorf("prepare: %w", err)
	}

	cc, err := c.chunkConn(pw.PrimaryAddress)
	if err != nil {
		return err
	}
	ch := godfsv1.NewChunkServiceClient(cc)
	ws, err := ch.WriteChunk(ctx)
	if err != nil {
		return err
	}
	if err := ws.Send(&godfsv1.WriteChunkRequest{
		Frame: &godfsv1.WriteChunkRequest_Meta{
			Meta: &godfsv1.WriteChunkMeta{
				ChunkId:            pw.ChunkId,
				OffsetInChunk:      pw.ChunkOffset,
				LeaseId:            pw.LeaseId,
				Version:            pw.Version,
				SecondaryAddresses: pw.SecondaryAddresses,
			},
		},
	}); err != nil {
		return err
	}
	slice := data[pos : pos+n]
	if err := ws.Send(&godfsv1.WriteChunkRequest{
		Frame: &godfsv1.WriteChunkRequest_Data{Data: slice},
	}); err != nil {
		return err
	}
	resp, err := ws.CloseAndRecv()
	if err != nil {
		return err
	}

	_, err = c.master.CommitChunk(ctx, &godfsv1.CommitChunkRequest{
		Path:           path,
		ChunkId:        pw.ChunkId,
		ChunkIndex:     pw.ChunkIndex,
		ChunkOffset:    pw.ChunkOffset,
		Written:        n,
		ChecksumSha256: resp.ChecksumSha256,
		Version:        pw.Version,
	})
	if err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func (c *Client) writeSegment(ctx context.Context, path string, data []byte, pos, n int64) error {
	return grpcRetry(ctx, 5, func() error {
		return c.writeSegmentOnce(ctx, path, data, pos, n)
	})
}

// Write writes full data to path (file must exist). Multiple chunk boundaries may be written in parallel (see GODFS_CLIENT_WRITE_PARALLELISM).
func (c *Client) Write(ctx context.Context, path string, data []byte) error {
	cs := c.chunkSize
	if cs <= 0 {
		cs = defaultChunkSize
	}
	var segs []writeSeg
	var pos int64
	remain := int64(len(data))
	for remain > 0 {
		chunkOff := pos % cs
		maxInChunk := cs - chunkOff
		n := remain
		if n > maxInChunk {
			n = maxInChunk
		}
		segs = append(segs, writeSeg{pos: pos, n: n})
		pos += n
		remain -= n
	}
	if len(segs) == 0 {
		return nil
	}

	par := c.writeParallelism()
	if par < 2 || len(segs) == 1 {
		for _, s := range segs {
			if err := c.writeSegment(ctx, path, data, s.pos, s.n); err != nil {
				return err
			}
		}
		return nil
	}

	sem := semaphore.NewWeighted(int64(par))
	g, ctx := errgroup.WithContext(ctx)
	for _, s := range segs {
		s := s
		g.Go(func() error {
			if err := sem.Acquire(ctx, 1); err != nil {
				return err
			}
			defer sem.Release(1)
			return c.writeSegment(ctx, path, data, s.pos, s.n)
		})
	}
	return g.Wait()
}
