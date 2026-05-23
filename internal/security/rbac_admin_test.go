package security

import "testing"

func TestPermFromMethod_AdminAndTruncate(t *testing.T) {
	adminMethods := []string{
		"/godfs.v1.MasterService/ListUnderReplicatedChunks",
		"/godfs.v1.MasterService/ListChunkNodes",
		"/godfs.v1.MasterService/RunRebalanceNow",
		"/godfs.v1.MasterService/CreateSnapshot",
		"/godfs.v1.MasterService/RestoreSnapshot",
		"/godfs.v1.MasterService/TruncateFile",
	}
	for _, m := range adminMethods[:5] {
		if PermFromMethod(m) != PermAdmin {
			t.Fatalf("%s: got %q want admin", m, PermFromMethod(m))
		}
	}
	if PermFromMethod(adminMethods[5]) != PermWrite {
		t.Fatalf("TruncateFile should require write, got %q", PermFromMethod(adminMethods[5]))
	}
}

func TestRBAC_AdminPrincipalVsUser(t *testing.T) {
	e, err := NewRBAC(`[{"principal":"op","path_prefix":"/","permissions":["admin"]},{"principal":"user","path_prefix":"/","permissions":["read","write"]}]`, false)
	if err != nil {
		t.Fatal(err)
	}
	if !e.Allowed("op", "/any", PermAdmin) {
		t.Fatal("op admin")
	}
	if e.Allowed("user", "/any", PermAdmin) {
		t.Fatal("user must not have admin")
	}
	if !e.Allowed("user", "/f", PermWrite) || !e.Allowed("user", "/f", PermRead) {
		t.Fatal("user read/write")
	}
}
