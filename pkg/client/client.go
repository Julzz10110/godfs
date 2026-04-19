package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc"

	godfsv1 "godfs/api/proto/godfs/v1"
	"godfs/internal/security"
)

const defaultChunkSize = 64 * 1024 * 1024

// Client is the goDFS client SDK.
type Client struct {
	master godfsv1.MasterServiceClient
	conn   *grpc.ClientConn

	chunkSize int64
	apiKey    string
	// writeParallel limits concurrent chunk writes when >0 (default from GODFS_CLIENT_WRITE_PARALLELISM or 4).
	writeParallel int

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
	return NewWithOptions(masterAddr, chunkSize, "")
}

// NewWithOptions is like [NewWithChunkSize] but sets an explicit API key/JWT bearer (overrides GODFS_CLIENT_API_KEY).
// TLS and credentials follow GODFS_TLS_* environment variables; see internal/security package.
func NewWithOptions(masterAddr string, chunkSize int64, apiKey string) (*Client, error) {
	opts, err := security.UserClientDialOptions(apiKey)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(masterAddr, opts...)
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
		apiKey:    apiKey,
		chCon:     map[string]*grpc.ClientConn{},
	}, nil
}

// NewGateway returns a client intended for an HTTP gateway: dial options do not read GODFS_CLIENT_API_KEY.
// Pass Bearer tokens per request using grpc metadata on the context.
func NewGateway(masterAddr string, chunkSize int64) (*Client, error) {
	opts, err := security.UserClientDialOptionsForGateway()
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(masterAddr, opts...)
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
		apiKey:    "",
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
	opts, err := security.UserClientDialOptions(c.apiKey)
	if err != nil {
		return nil, err
	}
	cc, err := grpc.NewClient(addr, opts...)
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
		var gr *godfsv1.GetChunkForReadResponse
		err := grpcRetry(ctx, 5, func() error {
			var e error
			gr, e = c.master.GetChunkForRead(ctx, &godfsv1.GetChunkForReadRequest{
				Path:   path,
				Offset: off,
			})
			return e
		})
		if err != nil {
			return nil, err
		}
		var reps []string
		if len(gr.ReplicaLocations) > 0 {
			for _, loc := range gr.ReplicaLocations {
				reps = append(reps, loc.GrpcAddress)
			}
		} else {
			reps = gr.ReplicaAddresses
		}
		if len(reps) == 0 {
			return nil, fmt.Errorf("no replicas")
		}
		var readErr error
		for _, rep := range reps {
			cc, err := c.chunkConn(rep)
			if err != nil {
				readErr = err
				continue
			}
			ch := godfsv1.NewChunkServiceClient(cc)
			var rc godfsv1.ChunkService_ReadChunkClient
			err = grpcRetry(ctx, 5, func() error {
				var e error
				rc, e = ch.ReadChunk(ctx, &godfsv1.ReadChunkRequest{
					ChunkId:       gr.ChunkId,
					OffsetInChunk: gr.ChunkOffset,
					Length:        gr.AvailableInChunk,
				})
				return e
			})
			if err != nil {
				readErr = err
				continue
			}
			dstOff := off
			var streamErr error
			var verifyHash hash.Hash
			if len(gr.ChunkChecksumSha256) == 32 && gr.ChunkOffset == 0 {
				verifyHash = sha256.New()
			}
			for {
				msg, err := rc.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					streamErr = err
					break
				}
				if verifyHash != nil {
					verifyHash.Write(msg.Data)
				}
				n := copy(out[dstOff:], msg.Data)
				dstOff += int64(n)
				if n < len(msg.Data) {
					streamErr = fmt.Errorf("short buffer")
					break
				}
			}
			if streamErr == nil && verifyHash != nil {
				if !bytes.Equal(verifyHash.Sum(nil), gr.ChunkChecksumSha256) {
					streamErr = fmt.Errorf("chunk checksum mismatch")
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
