//go:build linux

package main

import (
	"context"
	"errors"
	"strings"
	"syscall"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"godfs/internal/domain"
)

func grpcToErrno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	if errors.Is(err, context.Canceled) {
		return syscall.EINTR
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return syscall.ETIMEDOUT
	}
	code, msg, ok := rpcStatus(err)
	if !ok {
		return syscall.EIO
	}
	switch code {
	case codes.NotFound:
		return syscall.ENOENT
	case codes.AlreadyExists:
		return syscall.EEXIST
	case codes.PermissionDenied, codes.Unauthenticated:
		return syscall.EACCES
	case codes.ResourceExhausted:
		return syscall.ENOSPC
	case codes.InvalidArgument:
		return syscall.EINVAL
	case codes.Unavailable:
		return syscall.EAGAIN
	case codes.Aborted:
		return syscall.EIO
	case codes.FailedPrecondition:
		return failedPreToErrno(msg)
	default:
		return syscall.EIO
	}
}

func rpcStatus(err error) (codes.Code, string, bool) {
	for err != nil {
		if st, ok := status.FromError(err); ok {
			return st.Code(), st.Message(), true
		}
		err = errors.Unwrap(err)
	}
	return codes.Unknown, "", false
}

func failedPreToErrno(msg string) syscall.Errno {
	switch {
	case strings.Contains(msg, domain.ErrNotEmpty.Error()):
		return syscall.ENOTEMPTY
	case strings.Contains(msg, domain.ErrIsDir.Error()):
		return syscall.EISDIR
	case strings.Contains(msg, domain.ErrNotDir.Error()):
		return syscall.ENOTDIR
	case strings.Contains(msg, domain.ErrParentNotFound.Error()):
		return syscall.ENOENT
	case strings.Contains(msg, domain.ErrLeaseConflict.Error()):
		return syscall.EAGAIN
	case strings.Contains(msg, domain.ErrInsufficientChunkServers.Error()):
		return syscall.ENOSPC
	case strings.Contains(msg, "not leader"):
		return syscall.EAGAIN
	default:
		return syscall.EIO
	}
}
