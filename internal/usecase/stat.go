package usecase

import (
	"context"
	"time"
)

// StatInfo is namespace metadata returned by Stat.
type StatInfo struct {
	IsDir    bool
	Size     int64
	Created  time.Time
	Modified time.Time
	Mode     uint32
}

// Stat returns metadata for a file or directory path.
func Stat(ctx context.Context, store MasterStore, path string) (StatInfo, error) {
	p, err := NormalizeStatPath(path)
	if err != nil {
		return StatInfo{}, err
	}
	isDir, sz, cr, mod, mode, err := store.Stat(ctx, p)
	if err != nil {
		return StatInfo{}, err
	}
	return StatInfo{IsDir: isDir, Size: sz, Created: cr, Modified: mod, Mode: mode}, nil
}
