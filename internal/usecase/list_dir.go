package usecase

import (
	"context"
	"path"
)

// DirEntryInfo is one row in a directory listing.
type DirEntryInfo struct {
	Name  string
	IsDir bool
	Size  int64
}

// ListDir validates the directory path and returns enriched entries.
func ListDir(ctx context.Context, store MasterStore, dirPath string) ([]DirEntryInfo, error) {
	p, err := NormalizeFSDirPath(dirPath)
	if err != nil {
		return nil, err
	}
	names, _, err := store.ListDir(ctx, p)
	if err != nil {
		return nil, err
	}
	out := make([]DirEntryInfo, 0, len(names))
	for _, n := range names {
		full := path.Join(p, n)
		isDir, sz, _, _, _, err := store.Stat(ctx, full)
		if err != nil {
			continue
		}
		out = append(out, DirEntryInfo{Name: n, IsDir: isDir, Size: sz})
	}
	return out, nil
}
