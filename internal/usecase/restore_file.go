package usecase

import "context"

// RestoreFile undeletes a soft-deleted file (admin).
func RestoreFile(ctx context.Context, store MasterStore, path string) error {
	p, err := NormalizeFSPath(path)
	if err != nil {
		return err
	}
	return store.RestoreFile(ctx, p)
}
