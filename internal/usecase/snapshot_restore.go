package usecase

import (
	"context"
	"strings"

	godfsv1 "godfs/api/proto/godfs/v1"
	"godfs/internal/domain"
)

// RestoreSnapshot applies a backup manifest to metadata.
func RestoreSnapshot(ctx context.Context, store MasterStore, manifest *godfsv1.BackupManifest, force bool) error {
	man, err := BackupSnapshotFromProto(manifest)
	if err != nil {
		return err
	}
	return store.RestoreSnapshot(ctx, man, force)
}

// DeleteSnapshot removes a snapshot by id.
func DeleteSnapshot(ctx context.Context, store MasterStore, snapshotID string) error {
	if strings.TrimSpace(snapshotID) == "" {
		return domain.ErrInvalidSnapshotLabel
	}
	return store.DeleteSnapshot(ctx, snapshotID)
}

// ListSnapshots returns snapshot catalog entries.
func ListSnapshots(ctx context.Context, store MasterStore) ([]domain.SnapshotInfo, error) {
	return store.ListSnapshots(ctx)
}

// GetSnapshot returns a full backup manifest.
func GetSnapshot(ctx context.Context, store MasterStore, snapshotID string) (*domain.BackupSnapshot, error) {
	if strings.TrimSpace(snapshotID) == "" {
		return nil, domain.ErrInvalidSnapshotLabel
	}
	return store.GetSnapshot(ctx, snapshotID)
}
