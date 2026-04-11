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
		m.Audit.LogMaster(principal, short, path, oldPath, newPath, err == nil, errMsg)
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
	}
	return "/", "", ""
}

// NewChunkUnaryInterceptor requires GODFS_CLUSTER_KEY to match Bearer when cluster auth is enabled.
func NewChunkUnaryInterceptor(clusterKey string) grpc.UnaryServerInterceptor {
	enabled := strings.TrimSpace(clusterKey) != ""
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if !enabled {
			return handler(ctx, req)
		}
		if err := checkClusterBearer(ctx, clusterKey); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// NewChunkStreamInterceptor is the streaming counterpart for ChunkService.
func NewChunkStreamInterceptor(clusterKey string) grpc.StreamServerInterceptor {
	enabled := strings.TrimSpace(clusterKey) != ""
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if !enabled {
			return handler(srv, ss)
		}
		if err := checkClusterBearer(ss.Context(), clusterKey); err != nil {
			return err
		}
		return handler(srv, ss)
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
