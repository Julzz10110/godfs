package security

import (
	"testing"
)

func TestRBAC_ImplicitOpen(t *testing.T) {
	e, err := NewRBAC("", true)
	if err != nil {
		t.Fatal(err)
	}
	if !e.Allowed("alice", "/data/x", PermRead) {
		t.Fatal("expected read allowed")
	}
	if !e.Allowed("alice", "/data/x", PermWrite) {
		t.Fatal("expected write allowed")
	}
	if e.Allowed("alice", "/", PermNode) {
		t.Fatal("node must not be implicit for users")
	}
	if !e.Allowed(PrincipalCluster, "/", PermNode) {
		t.Fatal("cluster should have node")
	}
	if e.Allowed(PrincipalCluster, "/x", PermRead) {
		t.Fatal("cluster should not read paths")
	}
}

func TestRBAC_PrefixRules(t *testing.T) {
	raw := `[{"principal":"u1","path_prefix":"/a","permissions":["read"]},{"principal":"u1","path_prefix":"/a/b","permissions":["write"]}]`
	e, err := NewRBAC(raw, false)
	if err != nil {
		t.Fatal(err)
	}
	if !e.Allowed("u1", "/a/x", PermRead) {
		t.Fatal("read under /a")
	}
	if e.Allowed("u1", "/a/x", PermWrite) {
		t.Fatal("write not allowed at /a only")
	}
	if !e.Allowed("u1", "/a/b/c", PermWrite) {
		t.Fatal("longer prefix wins for write")
	}
	if e.Allowed("u2", "/a/x", PermRead) {
		t.Fatal("wrong principal")
	}
}

func TestRBAC_Rename(t *testing.T) {
	e, err := NewRBAC(`[{"principal":"u","path_prefix":"/","permissions":["write"]}]`, false)
	if err != nil {
		t.Fatal(err)
	}
	if !e.AllowedRename("u", "/a/old", "/b/new") {
		t.Fatal("rename allowed")
	}
	if e.AllowedRename(PrincipalCluster, "/a", "/b") {
		t.Fatal("cluster rename denied")
	}
}
