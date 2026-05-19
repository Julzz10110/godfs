package usecase

import "context"

// CreateSnapshot validates the label and delegates to the metadata store.
func CreateSnapshot(ctx context.Context, store MasterStore, label string) (snapshotID string, createdAtUnix int64, err error) {
	if err := ValidateSnapshotLabel(label); err != nil {
		return "", 0, err
	}
	return store.CreateSnapshot(ctx, label)
}
