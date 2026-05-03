package usecase

import (
	"errors"
	"strings"
	"testing"

	"godfs/internal/domain"
)

func TestValidateSnapshotLabel(t *testing.T) {
	t.Parallel()
	ok := []string{"e2e", "v1.2.3-nightly", strings.Repeat("a", 128)}
	for _, l := range ok {
		if err := ValidateSnapshotLabel(l); err != nil {
			t.Fatalf("ok %q: %v", l, err)
		}
	}
	bad := []string{
		"",
		strings.Repeat("b", 129),
		"x/y",
		"a\\b",
		"..\\x",
		"a\nb",
		"a\x00b",
	}
	for _, l := range bad {
		err := ValidateSnapshotLabel(l)
		if !errors.Is(err, domain.ErrInvalidSnapshotLabel) {
			t.Fatalf("bad %q: got %v want %v", l, err, domain.ErrInvalidSnapshotLabel)
		}
	}
}
