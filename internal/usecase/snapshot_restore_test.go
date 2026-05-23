package usecase

import (
	"context"
	"errors"
	"testing"

	godfsv1 "godfs/api/proto/godfs/v1"
	"godfs/internal/domain"
)

func TestRestoreSnapshot_requiresManifest(t *testing.T) {
	m := &mockMasterStore{}
	err := RestoreSnapshot(context.Background(), m, nil, false)
	if !errors.Is(err, ErrManifestRequired) {
		t.Fatalf("got %v", err)
	}
}

func TestRestoreSnapshot_passesForce(t *testing.T) {
	m := &mockMasterStore{}
	man := &godfsv1.BackupManifest{SnapshotId: "s1", CreatedAtUnix: 1, ChunkSizeBytes: 1024, ReplicationFactor: 1}
	if err := RestoreSnapshot(context.Background(), m, man, true); err != nil {
		t.Fatal(err)
	}
	if !m.restoreForce || m.restoreManifest == nil || m.restoreManifest.ID != "s1" {
		t.Fatalf("restore: force=%v manifest=%+v", m.restoreForce, m.restoreManifest)
	}
}

func TestDeleteSnapshot_emptyID(t *testing.T) {
	m := &mockMasterStore{}
	err := DeleteSnapshot(context.Background(), m, "  ")
	if !errors.Is(err, domain.ErrInvalidSnapshotLabel) {
		t.Fatalf("got %v", err)
	}
}

func TestRunRebalanceNow_capsSteps(t *testing.T) {
	m := &mockMasterStore{}
	n, err := RunRebalanceNow(context.Background(), m, 9999)
	if err != nil || n != maxRebalanceSteps || m.rebalanceSteps != maxRebalanceSteps {
		t.Fatalf("n=%d steps=%d err=%v", n, m.rebalanceSteps, err)
	}
}
