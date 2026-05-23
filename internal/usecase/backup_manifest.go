package usecase

import (
	"errors"
	"time"

	godfsv1 "godfs/api/proto/godfs/v1"
	"godfs/internal/domain"
)

// ErrManifestRequired is returned when RestoreSnapshot lacks a manifest.
var ErrManifestRequired = errors.New("manifest required")

// BackupSnapshotFromProto converts a REST/gRPC manifest to domain form.
func BackupSnapshotFromProto(m *godfsv1.BackupManifest) (*domain.BackupSnapshot, error) {
	if m == nil {
		return nil, ErrManifestRequired
	}
	out := &domain.BackupSnapshot{
		ID:                m.GetSnapshotId(),
		Label:             m.GetLabel(),
		CreatedAt:         time.Unix(m.GetCreatedAtUnix(), 0).UTC(),
		ChunkSize:         m.GetChunkSizeBytes(),
		ReplicationFactor: int(m.GetReplicationFactor()),
	}
	for _, f := range m.GetFiles() {
		if f == nil {
			continue
		}
		fe := domain.BackupFileEntry{
			Path:       f.GetPath(),
			Size:       f.GetSize(),
			Mode:       f.GetMode(),
			CreatedAt:  time.Unix(f.GetCreatedAtUnix(), 0).UTC(),
			ModifiedAt: time.Unix(f.GetModifiedAtUnix(), 0).UTC(),
		}
		for _, c := range f.GetChunks() {
			if c == nil {
				continue
			}
			reps := make([]domain.ChunkReplica, 0, len(c.GetReplicas()))
			for _, r := range c.GetReplicas() {
				if r == nil {
					continue
				}
				reps = append(reps, domain.ChunkReplica{
					NodeID:  domain.NodeID(r.GetNodeId()),
					Address: r.GetGrpcAddress(),
				})
			}
			fe.Chunks = append(fe.Chunks, domain.BackupChunkRef{
				ChunkID:    domain.ChunkID(c.GetChunkId()),
				ChunkIndex: c.GetChunkIndex(),
				Version:    c.GetVersion(),
				Checksum:   append([]byte(nil), c.GetChecksumSha256()...),
				Replicas:   reps,
			})
		}
		out.Files = append(out.Files, fe)
	}
	return out, nil
}
