package raftmeta

import (
	"context"
	"fmt"

	"godfs/internal/domain"
)

// TruncateFile sets file size on the leader (Raft mutation).
func (s *Service) TruncateFile(ctx context.Context, p string, size int64) ([]domain.ChunkDeleteInfo, error) {
	b, err := encodeCommand(cmdTruncateFile, struct {
		Path string
		Size int64
	}{Path: p, Size: size})
	if err != nil {
		return nil, err
	}
	resp, err := s.apply(ctx, b)
	if err != nil {
		return nil, err
	}
	infos, ok := resp.([]domain.ChunkDeleteInfo)
	if !ok {
		return nil, fmt.Errorf("unexpected truncate response: %T", resp)
	}
	return infos, nil
}
