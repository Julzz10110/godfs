package usecase

import (
	"testing"

	"godfs/internal/adapter/repository/metadata"
	"godfs/internal/raftmeta"
)

func TestMasterStoreImplementations(t *testing.T) {
	t.Helper()
	var _ MasterStore = (*metadata.Store)(nil)
	var _ MasterStore = (*raftmeta.Service)(nil)
}
