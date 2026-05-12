package grpc

import (
	"testing"

	"godfs/internal/raftmeta"
)

func TestRaftServiceImplementsRaftMembershipAdmin(t *testing.T) {
	t.Helper()
	var _ raftMembershipAdmin = (*raftmeta.Service)(nil)
}
