//go:build linux

package main

import (
	"errors"
	"syscall"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"godfs/internal/domain"
)

func TestGrpcToErrno(t *testing.T) {
	t.Parallel()
	cases := []struct {
		err  error
		want syscall.Errno
	}{
		{nil, 0},
		{status.Error(codes.NotFound, domain.ErrNotFound.Error()), syscall.ENOENT},
		{status.Error(codes.AlreadyExists, domain.ErrAlreadyExists.Error()), syscall.EEXIST},
		{status.Error(codes.PermissionDenied, "no"), syscall.EACCES},
		{status.Error(codes.ResourceExhausted, "full"), syscall.ENOSPC},
		{status.Error(codes.InvalidArgument, "bad"), syscall.EINVAL},
		{status.Error(codes.Unavailable, "down"), syscall.EAGAIN},
		{status.Error(codes.FailedPrecondition, domain.ErrNotEmpty.Error()), syscall.ENOTEMPTY},
		{status.Error(codes.FailedPrecondition, domain.ErrIsDir.Error()), syscall.EISDIR},
		{status.Error(codes.FailedPrecondition, domain.ErrNotDir.Error()), syscall.ENOTDIR},
		{status.Error(codes.FailedPrecondition, "not leader (leader_grpc=x)"), syscall.EAGAIN},
		{errors.New("plain"), syscall.EIO},
	}
	for _, tc := range cases {
		if got := grpcToErrno(tc.err); got != tc.want {
			t.Errorf("grpcToErrno(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}
