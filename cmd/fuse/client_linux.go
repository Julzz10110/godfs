//go:build linux

package main

import (
	"context"

	godfsv1 "godfs/api/proto/godfs/v1"
	"godfs/pkg/client"
)

// fuseCLI is the subset of pkg/client used by the FUSE adapter (enables tests with stubs).
type fuseCLI interface {
	Stat(ctx context.Context, path string) (*client.FileInfo, error)
	List(ctx context.Context, path string) ([]*godfsv1.DirEntry, error)
	Create(ctx context.Context, path string) error
	Mkdir(ctx context.Context, path string) error
	Delete(ctx context.Context, path string) error
	Rename(ctx context.Context, oldPath, newPath string) error
	ReadRange(ctx context.Context, path string, offset, length int64) ([]byte, error)
	WriteAt(ctx context.Context, path string, off int64, data []byte) error
	Truncate(ctx context.Context, path string, size int64) error
}
