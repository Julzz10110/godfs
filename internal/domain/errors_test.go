package domain

import (
	"errors"
	"testing"
)

func TestSentinelErrors(t *testing.T) {
	t.Parallel()
	cases := []error{
		ErrNotFound,
		ErrAlreadyExists,
		ErrNotEmpty,
		ErrIsDir,
		ErrNotDir,
		ErrInvalidPath,
		ErrNoChunkServer,
		ErrLeaseConflict,
		ErrChunkMismatch,
		ErrParentNotFound,
		ErrInsufficientChunkServers,
		ErrNotLeader,
	}
	for _, want := range cases {
		got := errors.New(want.Error())
		if errors.Is(got, want) {
			t.Errorf("errors.Is(new(%q), sentinel) should be false", want.Error())
		}
		if !errors.Is(want, want) {
			t.Errorf("errors.Is(sentinel, sentinel) for %v", want)
		}
	}
}
