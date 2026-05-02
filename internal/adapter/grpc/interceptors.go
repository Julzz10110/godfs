package grpc

import (
	"context"
	"strings"

	godfsv1 "godfs/api/proto/godfs/v1"
	"godfs/internal/security"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type masterInterceptors struct {
	Auth  *security.Auth
	RBAC  *security.RBAC
	Audit *security.AuditLogger
}

// NewMasterUnaryInterceptor enforces auth + RBAC + audit when auth is configured.
func NewMasterUnaryInterceptor(auth *security.Auth, rbac *security.RBAC, audit *security.AuditLogger) grpc.UnaryServerInterceptor {
	m := &masterInterceptors{Auth: auth, RBAC: rbac, Audit: audit}
	return m.unary
}

func (m *masterInterceptors) unary(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if m.Auth == nil || !m.Auth.Enabled {
		return handler(ctx, req)
	}
	principal, err := m.Auth.PrincipalFromContext(ctx)
	if err != nil {
		return nil, err
	}
	path, oldPath, newPath := masterPaths(info.FullMethod, req)
	want := security.PermFromMethod(info.FullMethod)

	if want == security.PermNode {
		if m.RBAC == nil || !m.RBAC.Allowed(principal, "/", security.PermNode) {
			return nil, status.Error(codes.PermissionDenied, "node permission required")
		}
	} else {
		if principal == security.PrincipalCluster {
			return nil, status.Error(codes.PermissionDenied, "cluster principal cannot call this method")
		}
		if info.FullMethod == "/godfs.v1.MasterService/Rename" {
			if m.RBAC == nil || !m.RBAC.AllowedRename(principal, oldPath, newPath) {
				return nil, status.Error(codes.PermissionDenied, "rename not allowed")
			}
		} else if !m.RBAC.Allowed(principal, path, want) {
			return nil, status.Error(codes.PermissionDenied, "forbidden")
		}
	}

	resp, err := handler(ctx, req)
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}
	if m.Audit != nil {
		short := info.FullMethod
		if i := strings.LastIndexByte(short, '/'); i >= 0 {
			short = short[i+1:]
		}
		rid := security.RequestIDFromIncomingContext(ctx)
		m.Audit.LogMaster(principal, short, path, oldPath, newPath, err == nil, errMsg, rid)
	}
	return resp, err
}

func masterPaths(fullMethod string, req interface{}) (path, oldPath, newPath string) {
	switch fullMethod {
	case "/godfs.v1.MasterService/CreateFile":
		if r, ok := req.(*godfsv1.CreateFileRequest); ok {
			return r.Path, "", ""
		}
	case "/godfs.v1.MasterService/Mkdir":
		if r, ok := req.(*godfsv1.MkdirRequest); ok {
			return r.Path, "", ""
		}
	case "/godfs.v1.MasterService/Delete":
		if r, ok := req.(*godfsv1.DeleteRequest); ok {
			return r.Path, "", ""
		}
	case "/godfs.v1.MasterService/Rename":
		if r, ok := req.(*godfsv1.RenameRequest); ok {
			return "", r.OldPath, r.NewPath
		}
	case "/godfs.v1.MasterService/Stat":
		if r, ok := req.(*godfsv1.StatRequest); ok {
			return r.Path, "", ""
		}
	case "/godfs.v1.MasterService/ListDir":
		if r, ok := req.(*godfsv1.ListDirRequest); ok {
			return r.Path, "", ""
		}
	case "/godfs.v1.MasterService/PrepareWrite":
		if r, ok := req.(*godfsv1.PrepareWriteRequest); ok {
			return r.Path, "", ""
		}
	case "/godfs.v1.MasterService/CommitChunk":
		if r, ok := req.(*godfsv1.CommitChunkRequest); ok {
			return r.Path, "", ""
		}
	case "/godfs.v1.MasterService/GetChunkForRead":
		if r, ok := req.(*godfsv1.GetChunkForReadRequest); ok {
			return r.Path, "", ""
		}
	case "/godfs.v1.MasterService/CreateSnapshot", "/godfs.v1.MasterService/ListSnapshots",
		"/godfs.v1.MasterService/GetSnapshot", "/godfs.v1.MasterService/DeleteSnapshot":
		return "/", "", ""
	}
	return "/", "", ""
}

func methodShort(full string) string {
	if i := strings.LastIndexByte(full, '/'); i >= 0 {
		return full[i+1:]
	}
	return full
}

func chunkPrincipal(clusterKey string) string {
	if strings.TrimSpace(clusterKey) != "" {
		return security.PrincipalCluster
	}
	return "anonymous"
}

func chunkUnaryChunkID(fullMethod string, req interface{}) string {
	switch fullMethod {
	case "/godfs.v1.ChunkService/DeleteChunk":
		if r, ok := req.(*godfsv1.DeleteChunkRequest); ok {
			return r.ChunkId
		}
	case "/godfs.v1.ChunkService/ChecksumChunk":
		if r, ok := req.(*godfsv1.ChecksumChunkRequest); ok {
			return r.ChunkId
		}
	}
	return ""
}

// NewChunkUnaryInterceptor requires GODFS_CLUSTER_KEY to match Bearer when cluster auth is enabled.
// Optional audit lines for ChunkService when chunkAudit is true and audit is non-nil.
func NewChunkUnaryInterceptor(clusterKey string, audit *security.AuditLogger, chunkAudit bool) grpc.UnaryServerInterceptor {
	enabled := strings.TrimSpace(clusterKey) != ""
	pr := chunkPrincipal(clusterKey)
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if enabled {
			if err := checkClusterBearer(ctx, clusterKey); err != nil {
				return nil, err
			}
		}
		resp, err := handler(ctx, req)
		if chunkAudit && audit != nil {
			errMsg := ""
			if err != nil {
				errMsg = err.Error()
			}
			rid := security.RequestIDFromIncomingContext(ctx)
			audit.LogChunk(pr, methodShort(info.FullMethod), chunkUnaryChunkID(info.FullMethod, req), false, err == nil, errMsg, rid)
		}
		return resp, err
	}
}

// NewChunkStreamInterceptor is the streaming counterpart for ChunkService.
func NewChunkStreamInterceptor(clusterKey string, audit *security.AuditLogger, chunkAudit bool) grpc.StreamServerInterceptor {
	enabled := strings.TrimSpace(clusterKey) != ""
	pr := chunkPrincipal(clusterKey)
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if enabled {
			if err := checkClusterBearer(ss.Context(), clusterKey); err != nil {
				return err
			}
		}
		err := handler(srv, ss)
		if chunkAudit && audit != nil {
			errMsg := ""
			if err != nil {
				errMsg = err.Error()
			}
			rid := security.RequestIDFromIncomingContext(ss.Context())
			audit.LogChunk(pr, methodShort(info.FullMethod), "", true, err == nil, errMsg, rid)
		}
		return err
	}
}

func checkClusterBearer(ctx context.Context, want string) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing metadata")
	}
	tok := bearerFromMD(md)
	if tok == "" {
		return status.Error(codes.Unauthenticated, "missing authorization")
	}
	if tok != want {
		return status.Error(codes.Unauthenticated, "invalid cluster credentials")
	}
	return nil
}

func bearerFromMD(md metadata.MD) string {
	for _, k := range []string{"authorization", "Authorization"} {
		v := md.Get(k)
		if len(v) == 0 {
			continue
		}
		tok := strings.TrimSpace(v[0])
		if len(tok) >= 7 && strings.EqualFold(tok[:7], "bearer ") {
			return strings.TrimSpace(tok[7:])
		}
		return tok
	}
	return ""
}
