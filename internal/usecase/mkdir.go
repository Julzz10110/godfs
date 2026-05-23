package usecase

import "context"

// Mkdir validates the directory path and creates it in metadata.
func Mkdir(ctx context.Context, store MasterStore, path string) error {
	p, err := NormalizeFSDirPath(path)
	if err != nil {
		return err
	}
	return store.Mkdir(ctx, p)
}
