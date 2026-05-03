package client

import (
	"context"
	"fmt"
	"io"
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

func (c *Client) writeBytesOnce(ctx context.Context, path string, pos int64, b []byte) error {
	if len(b) == 0 {
		return nil
	}
	n := int64(len(b))
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
	if err := ws.Send(&godfsv1.WriteChunkRequest{
		Frame: &godfsv1.WriteChunkRequest_Data{Data: b},
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

func (c *Client) writeBytes(ctx context.Context, path string, pos int64, b []byte) error {
	return grpcRetry(ctx, 5, func() error {
		return c.writeBytesOnce(ctx, path, pos, b)
	})
}

// WriteAt writes data to an existing file starting at offset (sparse extend supported by the server).
func (c *Client) WriteAt(ctx context.Context, path string, off int64, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if off < 0 {
		return fmt.Errorf("invalid offset")
	}
	cs := c.chunkSize
	if cs <= 0 {
		cs = defaultChunkSize
	}
	var segs []writeSeg
	pos := off
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
	for _, s := range segs {
		bufLo := s.pos - off
		bufHi := bufLo + s.n
		if bufLo < 0 || bufHi > int64(len(data)) {
			return fmt.Errorf("write segment bounds")
		}
		chunk := data[bufLo:bufHi]
		if err := c.writeBytes(ctx, path, s.pos, chunk); err != nil {
			return err
		}
	}
	return nil
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

// WriteFromReader writes bytes from r to path starting at offset 0, without buffering the entire body in memory.
// At most one chunk (chunkSize) is buffered at a time.
func (c *Client) WriteFromReader(ctx context.Context, path string, r io.Reader) error {
	cs := c.chunkSize
	if cs <= 0 {
		cs = defaultChunkSize
	}
	if cs < 1 {
		cs = 1
	}
	if cs > int64(int(^uint(0)>>1)) { // guard int overflow on 32-bit (unlikely here, but cheap)
		cs = int64(int(^uint(0) >> 1))
	}

	buf := make([]byte, int(cs))
	var filled int
	var pos int64
	for {
		nr, err := r.Read(buf[filled:])
		if nr > 0 {
			filled += nr
			if filled == len(buf) {
				if err := c.writeBytes(ctx, path, pos, buf); err != nil {
					return err
				}
				pos += int64(filled)
				filled = 0
			}
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
	}
	if filled > 0 {
		if err := c.writeBytes(ctx, path, pos, buf[:filled]); err != nil {
			return err
		}
	}
	return nil
}
