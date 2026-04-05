package client

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	godfsv1 "godfs/api/proto/godfs/v1"
)

const defaultChunkSize = 64 * 1024 * 1024

// Client is the goDFS SDK (Phase 1).
type Client struct {
	master godfsv1.MasterServiceClient
	conn   *grpc.ClientConn

	chunkSize int64

	mu    sync.Mutex
	chCon map[string]*grpc.ClientConn
}

// New connects to the master at masterAddr (e.g. "localhost:9090").
// Chunk size for splitting writes defaults to 64 MiB; it must match the master's chunk size.
// Use [NewWithChunkSize] when the master uses a different value.
func New(masterAddr string) (*Client, error) {
	return NewWithChunkSize(masterAddr, defaultChunkSize)
}

// NewWithChunkSize is like [New] but sets the client-side chunk boundary used for splitting writes.
func NewWithChunkSize(masterAddr string, chunkSize int64) (*Client, error) {
	conn, err := grpc.NewClient(masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	if chunkSize <= 0 {
		chunkSize = defaultChunkSize
	}
	return &Client{
		master:    godfsv1.NewMasterServiceClient(conn),
		conn:      conn,
		chunkSize: chunkSize,
		chCon:     map[string]*grpc.ClientConn{},
	}, nil
}

// Close releases resources.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, cc := range c.chCon {
		_ = cc.Close()
	}
	c.chCon = nil
	return c.conn.Close()
}

func (c *Client) chunkConn(addr string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cc, ok := c.chCon[addr]; ok {
		return cc, nil
	}
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c.chCon[addr] = cc
	return cc, nil
}

// Create creates an empty file (parent directory must exist).
func (c *Client) Create(ctx context.Context, path string) error {
	_, err := c.master.CreateFile(ctx, &godfsv1.CreateFileRequest{Path: path})
	return err
}

// Mkdir creates a directory.
func (c *Client) Mkdir(ctx context.Context, path string) error {
	_, err := c.master.Mkdir(ctx, &godfsv1.MkdirRequest{Path: path})
	return err
}

// Delete removes a file or empty directory.
func (c *Client) Delete(ctx context.Context, path string) error {
	_, err := c.master.Delete(ctx, &godfsv1.DeleteRequest{Path: path})
	return err
}

// Rename renames a file or directory.
func (c *Client) Rename(ctx context.Context, oldPath, newPath string) error {
	_, err := c.master.Rename(ctx, &godfsv1.RenameRequest{OldPath: oldPath, NewPath: newPath})
	return err
}

// FileInfo mirrors stat result.
type FileInfo struct {
	IsDir    bool
	Size     int64
	Mode     uint32
	ModTime  time.Time
	CreateAt time.Time
}

// Stat returns metadata.
func (c *Client) Stat(ctx context.Context, path string) (*FileInfo, error) {
	r, err := c.master.Stat(ctx, &godfsv1.StatRequest{Path: path})
	if err != nil {
		return nil, err
	}
	return &FileInfo{
		IsDir:    r.IsDir,
		Size:     r.Size,
		Mode:     r.Mode,
		ModTime:  time.Unix(r.ModifiedAtUnix, 0),
		CreateAt: time.Unix(r.CreatedAtUnix, 0),
	}, nil
}

// List returns directory entries (names only minimal API; extend as needed).
func (c *Client) List(ctx context.Context, path string) ([]*godfsv1.DirEntry, error) {
	r, err := c.master.ListDir(ctx, &godfsv1.ListDirRequest{Path: path})
	if err != nil {
		return nil, err
	}
	return r.Entries, nil
}

// Write writes full data to path (file must exist).
func (c *Client) Write(ctx context.Context, path string, data []byte) error {
	var pos int64
	remain := int64(len(data))
	for remain > 0 {
		cs := c.chunkSize
		if cs <= 0 {
			cs = defaultChunkSize
		}
		chunkOff := pos % cs
		maxInChunk := cs - chunkOff
		n := remain
		if n > maxInChunk {
			n = maxInChunk
		}

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
					ChunkId:             pw.ChunkId,
					OffsetInChunk:       pw.ChunkOffset,
					LeaseId:             pw.LeaseId,
					Version:             pw.Version,
					SecondaryAddresses:  pw.SecondaryAddresses,
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

		pos += n
		remain -= n
	}
	return nil
}

// Read reads entire file.
func (c *Client) Read(ctx context.Context, path string) ([]byte, error) {
	st, err := c.Stat(ctx, path)
	if err != nil {
		return nil, err
	}
	if st.IsDir {
		return nil, fmt.Errorf("is directory")
	}
	if st.Size == 0 {
		return nil, nil
	}

	out := make([]byte, st.Size)
	var off int64
	for off < st.Size {
		gr, err := c.master.GetChunkForRead(ctx, &godfsv1.GetChunkForReadRequest{
			Path:   path,
			Offset: off,
		})
		if err != nil {
			return nil, err
		}
		if len(gr.ReplicaAddresses) == 0 {
			return nil, fmt.Errorf("no replicas")
		}
		var readErr error
		for _, rep := range gr.ReplicaAddresses {
			cc, err := c.chunkConn(rep)
			if err != nil {
				readErr = err
				continue
			}
			ch := godfsv1.NewChunkServiceClient(cc)
			rc, err := ch.ReadChunk(ctx, &godfsv1.ReadChunkRequest{
				ChunkId:         gr.ChunkId,
				OffsetInChunk:   gr.ChunkOffset,
				Length:          gr.AvailableInChunk,
			})
			if err != nil {
				readErr = err
				continue
			}
			dstOff := off
			var streamErr error
			for {
				msg, err := rc.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					streamErr = err
					break
				}
				n := copy(out[dstOff:], msg.Data)
				dstOff += int64(n)
				if n < len(msg.Data) {
					streamErr = fmt.Errorf("short buffer")
					break
				}
			}
			if streamErr == nil {
				readErr = nil
				break
			}
			readErr = streamErr
		}
		if readErr != nil {
			return nil, readErr
		}
		off += gr.AvailableInChunk
	}
	return out, nil
}
