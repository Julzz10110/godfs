package usecase

import "context"

// Rename validates paths and moves a file or directory in metadata.
func Rename(ctx context.Context, store MasterStore, oldPath, newPath string) error {
	oldP, err := NormalizeRenamePath(oldPath)
	if err != nil {
		return err
	}
	newP, err := NormalizeRenamePath(newPath)
	if err != nil {
		return err
	}
	return store.Rename(ctx, oldP, newP)
}
