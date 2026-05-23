package client

import (
	"context"

	godfsv1 "godfs/api/proto/godfs/v1"
)

// Truncate sets the file size (sparse extend or shrink).
func (c *Client) Truncate(ctx context.Context, path string, size int64) error {
	return grpcRetry(ctx, 5, func() error {
		_, err := c.master.TruncateFile(ctx, &godfsv1.TruncateFileRequest{
			Path: path,
			Size: size,
		})
		return err
	})
}
