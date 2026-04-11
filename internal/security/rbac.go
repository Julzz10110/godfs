package security

import (
	"strings"
)

// Perm is a coarse capability for namespace paths.
type Perm string

const (
	PermRead   Perm = "read"
	PermWrite  Perm = "write"
	PermDelete Perm = "delete"
	PermAdmin  Perm = "admin"
	PermNode   Perm = "node" // RegisterNode, Heartbeat only (cluster / ops)
)

// RBACRule binds a principal to a path prefix and permissions.
type RBACRule struct {
	Principal   string   `json:"principal"`
	PathPrefix  string   `json:"path_prefix"`
	Permissions []string `json:"permissions"`
}

// RBAC evaluates path-scoped rules; empty rules + implicitOpen allows any authenticated non-cluster principal full file access.
type RBAC struct {
	Rules        []RBACRule
	ImplicitOpen bool // no GODFS_RBAC_JSON: allow read/write/delete for users; cluster gets node-only via MasterAuth
}

// NewRBAC builds an engine; if raw JSON is empty and implicitOpen, dev-friendly defaults apply when auth is enabled.
func NewRBAC(raw string, implicitOpen bool) (*RBAC, error) {
	rules, err := ParseRBACRules(raw)
	if err != nil {
		return nil, err
	}
	return &RBAC{Rules: rules, ImplicitOpen: implicitOpen}, nil
}

func (e *RBAC) ruleFor(principal, p string) *RBACRule {
	bestLen := -1
	var best *RBACRule
	for i := range e.Rules {
		r := &e.Rules[i]
		if r.Principal != principal && r.Principal != "*" {
			continue
		}
		prefix := r.PathPrefix
		if prefix == "" {
			prefix = "/"
		}
		if !strings.HasPrefix(p, prefix) {
			continue
		}
		if len(prefix) > bestLen {
			bestLen = len(prefix)
			best = r
		}
	}
	return best
}

func ruleHas(r *RBACRule, want Perm) bool {
	if r == nil {
		return false
	}
	for _, p := range r.Permissions {
		if Perm(p) == PermAdmin || p == string(PermAdmin) {
			return true
		}
		if Perm(p) == want || p == string(want) {
			return true
		}
	}
	return false
}

// Allowed checks whether principal may perform perm on path (namespace path, always starts with /).
func (e *RBAC) Allowed(principal string, path string, want Perm) bool {
	if principal == PrincipalCluster {
		return want == PermNode || want == PermAdmin
	}
	if len(e.Rules) == 0 && e.ImplicitOpen {
		switch want {
		case PermRead, PermWrite, PermDelete, PermAdmin:
			return true
		case PermNode:
			return false
		default:
			return false
		}
	}
	r := e.ruleFor(principal, path)
	return ruleHas(r, want) || ruleHas(r, PermAdmin)
}

// AllowedRename requires write on both paths when rules exist; with implicitOpen, write once is enough (both allowed).
func (e *RBAC) AllowedRename(principal, oldPath, newPath string) bool {
	if principal == PrincipalCluster {
		return false
	}
	if len(e.Rules) == 0 && e.ImplicitOpen {
		return true
	}
	return e.Allowed(principal, oldPath, PermWrite) && e.Allowed(principal, newPath, PermWrite)
}

// PermFromMethod returns required permission for a Master gRPC full method name.
func PermFromMethod(fullMethod string) Perm {
	switch fullMethod {
	case "/godfs.v1.MasterService/RegisterNode", "/godfs.v1.MasterService/Heartbeat":
		return PermNode
	case "/godfs.v1.MasterService/CreateFile", "/godfs.v1.MasterService/Mkdir",
		"/godfs.v1.MasterService/PrepareWrite", "/godfs.v1.MasterService/CommitChunk":
		return PermWrite
	case "/godfs.v1.MasterService/Delete":
		return PermDelete
	case "/godfs.v1.MasterService/Rename":
		return PermWrite // two paths checked separately
	case "/godfs.v1.MasterService/Stat", "/godfs.v1.MasterService/ListDir",
		"/godfs.v1.MasterService/GetChunkForRead":
		return PermRead
	default:
		return PermAdmin
	}
}

// IsRename reports whether the method is Rename (needs two path checks).
func IsRename(fullMethod string) bool {
	return fullMethod == "/godfs.v1.MasterService/Rename"
}

