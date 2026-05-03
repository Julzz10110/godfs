package usecase

import (
	"unicode"

	"godfs/internal/domain"
)

const snapshotLabelMaxLen = 128

// ValidateSnapshotLabel enforces safe labels for CreateSnapshot (REST/gRPC).
func ValidateSnapshotLabel(label string) error {
	if label == "" {
		return domain.ErrInvalidSnapshotLabel
	}
	if len(label) > snapshotLabelMaxLen {
		return domain.ErrInvalidSnapshotLabel
	}
	for _, r := range label {
		if r == '\n' || r == '\r' || r == 0 {
			return domain.ErrInvalidSnapshotLabel
		}
		if r == '/' || r == '\\' {
			return domain.ErrInvalidSnapshotLabel
		}
		if unicode.IsControl(r) {
			return domain.ErrInvalidSnapshotLabel
		}
	}
	return nil
}
