package client

import (
	"context"

	godfsv1 "godfs/api/proto/godfs/v1"
)

// CreateSnapshot captures a point-in-time metadata manifest for backup (FR-10).
func (c *Client) CreateSnapshot(ctx context.Context, label string) (snapshotID string, createdAtUnix int64, err error) {
	var r *godfsv1.CreateSnapshotResponse
	err = grpcRetry(ctx, 5, func() error {
		var e error
		r, e = c.master.CreateSnapshot(ctx, &godfsv1.CreateSnapshotRequest{Label: label})
		return e
	})
	if err != nil {
		return "", 0, err
	}
	return r.GetSnapshotId(), r.GetCreatedAtUnix(), nil
}

// ListSnapshots returns short entries for all snapshots.
func (c *Client) ListSnapshots(ctx context.Context) ([]*godfsv1.SnapshotListEntry, error) {
	var r *godfsv1.ListSnapshotsResponse
	err := grpcRetry(ctx, 5, func() error {
		var e error
		r, e = c.master.ListSnapshots(ctx, &godfsv1.ListSnapshotsRequest{})
		return e
	})
	if err != nil {
		return nil, err
	}
	return r.GetSnapshots(), nil
}

// GetSnapshot loads the full manifest for a snapshot id.
func (c *Client) GetSnapshot(ctx context.Context, snapshotID string) (*godfsv1.BackupManifest, error) {
	var r *godfsv1.GetSnapshotResponse
	err := grpcRetry(ctx, 5, func() error {
		var e error
		r, e = c.master.GetSnapshot(ctx, &godfsv1.GetSnapshotRequest{SnapshotId: snapshotID})
		return e
	})
	if err != nil {
		return nil, err
	}
	return r.GetManifest(), nil
}

// DeleteSnapshot removes snapshot metadata from the master (does not delete chunk bytes).
func (c *Client) DeleteSnapshot(ctx context.Context, snapshotID string) error {
	return grpcRetry(ctx, 5, func() error {
		_, err := c.master.DeleteSnapshot(ctx, &godfsv1.DeleteSnapshotRequest{SnapshotId: snapshotID})
		return err
	})
}
