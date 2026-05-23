package grpc

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	godfsv1 "godfs/api/proto/godfs/v1"
	"godfs/internal/domain"
	"godfs/internal/security"
	"godfs/internal/usecase"
)

// MasterServer implements godfsv1.MasterServiceServer.
type MasterServer struct {
	godfsv1.UnimplementedMasterServiceServer
	Store usecase.MasterStore
}

type raftMembershipAdmin interface {
	ListMasters(ctx context.Context) ([]domain.MasterPeer, domain.NodeID, error)
	AddMaster(ctx context.Context, nodeID domain.NodeID, raftAddr, grpcAddr string) error
	RemoveMaster(ctx context.Context, nodeID domain.NodeID) error
}

type leaderAware interface {
	IsLeader() bool
	LeaderGRPCAddr() string
}

func (m *MasterServer) ensureLeader() error {
	la, ok := m.Store.(leaderAware)
	if !ok {
		return nil // single-master mode
	}
	if la.IsLeader() {
		return nil
	}
	ldr := la.LeaderGRPCAddr()
	if ldr != "" {
		return status.Errorf(codes.FailedPrecondition, "not leader (leader_grpc=%s)", ldr)
	}
	return status.Error(codes.FailedPrecondition, "not leader")
}

func mapErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, domain.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, domain.ErrAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, domain.ErrNotEmpty):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, domain.ErrIsDir):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, domain.ErrNotDir):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, domain.ErrInvalidPath):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, domain.ErrInvalidSnapshotLabel):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, domain.ErrNoChunkServer):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, domain.ErrInsufficientChunkServers):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, domain.ErrParentNotFound):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, domain.ErrChunkMismatch):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, domain.ErrNotLeader):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func (m *MasterServer) RegisterNode(ctx context.Context, req *godfsv1.RegisterNodeRequest) (*godfsv1.RegisterNodeResponse, error) {
	if err := m.ensureLeader(); err != nil {
		return nil, err
	}
	if err := usecase.RegisterNode(ctx, m.Store, req.GetNodeId(), req.GetGrpcAddress(), req.GetCapacityBytes()); err != nil {
		return nil, mapErr(err)
	}
	return &godfsv1.RegisterNodeResponse{}, nil
}

func (m *MasterServer) CreateFile(ctx context.Context, req *godfsv1.CreateFileRequest) (*godfsv1.CreateFileResponse, error) {
	if err := m.ensureLeader(); err != nil {
		return nil, err
	}
	id, err := usecase.CreateFile(ctx, m.Store, req.GetPath())
	if err != nil {
		return nil, mapErr(err)
	}
	return &godfsv1.CreateFileResponse{FileId: string(id)}, nil
}

func (m *MasterServer) Mkdir(ctx context.Context, req *godfsv1.MkdirRequest) (*godfsv1.MkdirResponse, error) {
	if err := m.ensureLeader(); err != nil {
		return nil, err
	}
	if err := usecase.Mkdir(ctx, m.Store, req.GetPath()); err != nil {
		return nil, mapErr(err)
	}
	return &godfsv1.MkdirResponse{}, nil
}

func (m *MasterServer) Delete(ctx context.Context, req *godfsv1.DeleteRequest) (*godfsv1.DeleteResponse, error) {
	if err := m.ensureLeader(); err != nil {
		return nil, err
	}
	chunks, err := usecase.DeleteFile(ctx, m.Store, req.Path)
	if err != nil {
		return nil, mapErr(err)
	}
	for _, ch := range chunks {
		for _, addr := range ch.Replicas {
			if err := deleteChunkOnPeer(ctx, addr, string(ch.ChunkID)); err != nil {
				return nil, status.Errorf(codes.Internal, "delete chunk %s on %s: %v", ch.ChunkID, addr, err)
			}
		}
	}
	return &godfsv1.DeleteResponse{}, nil
}

func (m *MasterServer) RestoreFile(ctx context.Context, req *godfsv1.RestoreFileRequest) (*godfsv1.RestoreFileResponse, error) {
	if err := m.ensureLeader(); err != nil {
		return nil, err
	}
	if err := usecase.RestoreFile(ctx, m.Store, req.GetPath()); err != nil {
		return nil, mapErr(err)
	}
	return &godfsv1.RestoreFileResponse{}, nil
}

func deleteChunkOnPeer(ctx context.Context, addr, chunkID string) error {
	var last error
	for attempt := range 4 {
		if attempt > 0 {
			t := time.NewTimer(time.Duration(50*attempt) * time.Millisecond)
			select {
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			case <-t.C:
			}
		}
		last = deleteChunkOnce(ctx, addr, chunkID)
		if last == nil {
			return nil
		}
	}
	return last
}

func deleteChunkOnce(ctx context.Context, addr, chunkID string) error {
	dopts, err := security.ClientDialOptions()
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient(addr, dopts...)
	if err != nil {
		return err
	}
	defer conn.Close()
	cli := godfsv1.NewChunkServiceClient(conn)
	_, err = cli.DeleteChunk(ctx, &godfsv1.DeleteChunkRequest{ChunkId: chunkID})
	return err
}

func (m *MasterServer) TruncateFile(ctx context.Context, req *godfsv1.TruncateFileRequest) (*godfsv1.TruncateFileResponse, error) {
	if err := m.ensureLeader(); err != nil {
		return nil, err
	}
	chunks, err := usecase.TruncateFile(ctx, m.Store, req.GetPath(), req.GetSize())
	if err != nil {
		return nil, mapErr(err)
	}
	for _, ch := range chunks {
		for _, addr := range ch.Replicas {
			if err := deleteChunkOnPeer(ctx, addr, string(ch.ChunkID)); err != nil {
				return nil, status.Errorf(codes.Internal, "delete chunk %s on %s: %v", ch.ChunkID, addr, err)
			}
		}
	}
	return &godfsv1.TruncateFileResponse{}, nil
}

func (m *MasterServer) Rename(ctx context.Context, req *godfsv1.RenameRequest) (*godfsv1.RenameResponse, error) {
	if err := m.ensureLeader(); err != nil {
		return nil, err
	}
	if err := usecase.Rename(ctx, m.Store, req.GetOldPath(), req.GetNewPath()); err != nil {
		return nil, mapErr(err)
	}
	return &godfsv1.RenameResponse{}, nil
}

func (m *MasterServer) Stat(ctx context.Context, req *godfsv1.StatRequest) (*godfsv1.StatResponse, error) {
	info, err := usecase.Stat(ctx, m.Store, req.GetPath())
	if err != nil {
		return nil, mapErr(err)
	}
	return &godfsv1.StatResponse{
		IsDir:          info.IsDir,
		Size:           info.Size,
		CreatedAtUnix:  info.Created.Unix(),
		ModifiedAtUnix: info.Modified.Unix(),
		Mode:           info.Mode,
	}, nil
}

func (m *MasterServer) ListDir(ctx context.Context, req *godfsv1.ListDirRequest) (*godfsv1.ListDirResponse, error) {
	entries, err := usecase.ListDir(ctx, m.Store, req.GetPath())
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]*godfsv1.DirEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, &godfsv1.DirEntry{Name: e.Name, IsDir: e.IsDir, Size: e.Size})
	}
	return &godfsv1.ListDirResponse{Entries: out}, nil
}

func (m *MasterServer) PrepareWrite(ctx context.Context, req *godfsv1.PrepareWriteRequest) (*godfsv1.PrepareWriteResponse, error) {
	if err := m.ensureLeader(); err != nil {
		return nil, err
	}
	cid, addr, sec, primaryID, lease, idx, off, csize, ver, err := usecase.PrepareWrite(ctx, m.Store, req.Path, req.Offset, req.Length)
	if err != nil {
		return nil, mapErr(err)
	}
	return &godfsv1.PrepareWriteResponse{
		ChunkId:            string(cid),
		PrimaryAddress:     addr,
		SecondaryAddresses: sec,
		PrimaryNodeId:      string(primaryID),
		LeaseId:            string(lease),
		ChunkIndex:         idx,
		ChunkOffset:        off,
		ChunkSize:          csize,
		Version:            ver,
	}, nil
}

func (m *MasterServer) CommitChunk(ctx context.Context, req *godfsv1.CommitChunkRequest) (*godfsv1.CommitChunkResponse, error) {
	if err := m.ensureLeader(); err != nil {
		return nil, err
	}
	if err := usecase.CommitChunk(ctx, m.Store, req.GetPath(), domain.ChunkID(req.GetChunkId()),
		req.GetChunkIndex(), req.GetChunkOffset(), req.GetWritten(), req.GetChecksumSha256(), req.GetVersion()); err != nil {
		return nil, mapErr(err)
	}
	return &godfsv1.CommitChunkResponse{}, nil
}

func (m *MasterServer) GetChunkForRead(ctx context.Context, req *godfsv1.GetChunkForReadRequest) (*godfsv1.GetChunkForReadResponse, error) {
	plan, err := usecase.GetChunkForRead(ctx, m.Store, req.GetPath(), req.GetOffset())
	if err != nil {
		return nil, mapErr(err)
	}
	addrs := make([]string, len(plan.ReplicaLocs))
	protoLocs := make([]*godfsv1.ReplicaLocation, len(plan.ReplicaLocs))
	for i, r := range plan.ReplicaLocs {
		addrs[i] = r.Address
		protoLocs[i] = &godfsv1.ReplicaLocation{
			NodeId:      string(r.NodeID),
			GrpcAddress: r.Address,
		}
	}
	return &godfsv1.GetChunkForReadResponse{
		ChunkId:             string(plan.ChunkID),
		ReplicaAddresses:    addrs,
		ReplicaLocations:    protoLocs,
		ChunkOffset:         plan.ChunkOffset,
		AvailableInChunk:    plan.Available,
		Version:             plan.Version,
		ChunkChecksumSha256: plan.Checksum,
	}, nil
}

func (m *MasterServer) Heartbeat(ctx context.Context, req *godfsv1.HeartbeatRequest) (*godfsv1.HeartbeatResponse, error) {
	if err := m.ensureLeader(); err != nil {
		return nil, err
	}
	if err := usecase.Heartbeat(ctx, m.Store, req.GetNodeId(), req.GetCapacityBytes(), req.GetUsedBytes()); err != nil {
		return nil, mapErr(err)
	}
	return &godfsv1.HeartbeatResponse{ServerTimeUnix: time.Now().UTC().Unix()}, nil
}

func (m *MasterServer) CreateSnapshot(ctx context.Context, req *godfsv1.CreateSnapshotRequest) (*godfsv1.CreateSnapshotResponse, error) {
	if err := m.ensureLeader(); err != nil {
		return nil, err
	}
	id, ts, err := usecase.CreateSnapshot(ctx, m.Store, req.GetLabel())
	if err != nil {
		return nil, mapErr(err)
	}
	return &godfsv1.CreateSnapshotResponse{SnapshotId: id, CreatedAtUnix: ts}, nil
}

func (m *MasterServer) ListSnapshots(ctx context.Context, _ *godfsv1.ListSnapshotsRequest) (*godfsv1.ListSnapshotsResponse, error) {
	entries, err := usecase.ListSnapshots(ctx, m.Store)
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]*godfsv1.SnapshotListEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, &godfsv1.SnapshotListEntry{
			SnapshotId:    e.ID,
			Label:         e.Label,
			CreatedAtUnix: e.CreatedAtUnix,
			FileCount:     e.FileCount,
		})
	}
	return &godfsv1.ListSnapshotsResponse{Snapshots: out}, nil
}

func (m *MasterServer) GetSnapshot(ctx context.Context, req *godfsv1.GetSnapshotRequest) (*godfsv1.GetSnapshotResponse, error) {
	sn, err := usecase.GetSnapshot(ctx, m.Store, req.GetSnapshotId())
	if err != nil {
		return nil, mapErr(err)
	}
	return &godfsv1.GetSnapshotResponse{Manifest: backupSnapshotToProto(sn)}, nil
}

func (m *MasterServer) DeleteSnapshot(ctx context.Context, req *godfsv1.DeleteSnapshotRequest) (*godfsv1.DeleteSnapshotResponse, error) {
	if err := m.ensureLeader(); err != nil {
		return nil, err
	}
	if err := usecase.DeleteSnapshot(ctx, m.Store, req.GetSnapshotId()); err != nil {
		return nil, mapErr(err)
	}
	return &godfsv1.DeleteSnapshotResponse{}, nil
}

func (m *MasterServer) RestoreSnapshot(ctx context.Context, req *godfsv1.RestoreSnapshotRequest) (*godfsv1.RestoreSnapshotResponse, error) {
	if err := m.ensureLeader(); err != nil {
		return nil, err
	}
	if err := usecase.RestoreSnapshot(ctx, m.Store, req.GetManifest(), req.GetForce()); err != nil {
		if errors.Is(err, usecase.ErrManifestRequired) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, mapErr(err)
	}
	return &godfsv1.RestoreSnapshotResponse{}, nil
}

func (m *MasterServer) ListMasters(ctx context.Context, _ *godfsv1.ListMastersRequest) (*godfsv1.ListMastersResponse, error) {
	if err := m.ensureLeader(); err != nil {
		return nil, err
	}
	admin, ok := m.Store.(raftMembershipAdmin)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "raft membership admin not supported")
	}
	peers, leaderID, err := admin.ListMasters(ctx)
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]*godfsv1.MasterPeer, 0, len(peers))
	for i := range peers {
		p := peers[i]
		out = append(out, &godfsv1.MasterPeer{
			NodeId:      string(p.NodeID),
			RaftAddress: p.RaftAddress,
			GrpcAddress: p.GRPCAddress,
			Voter:       p.Voter,
		})
	}
	return &godfsv1.ListMastersResponse{Masters: out, LeaderNodeId: string(leaderID)}, nil
}

func (m *MasterServer) AddMaster(ctx context.Context, req *godfsv1.AddMasterRequest) (*godfsv1.AddMasterResponse, error) {
	if err := m.ensureLeader(); err != nil {
		return nil, err
	}
	admin, ok := m.Store.(raftMembershipAdmin)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "raft membership admin not supported")
	}
	if err := admin.AddMaster(ctx, domain.NodeID(req.GetNodeId()), req.GetRaftAddress(), req.GetGrpcAddress()); err != nil {
		return nil, mapErr(err)
	}
	return &godfsv1.AddMasterResponse{}, nil
}

func (m *MasterServer) RemoveMaster(ctx context.Context, req *godfsv1.RemoveMasterRequest) (*godfsv1.RemoveMasterResponse, error) {
	if err := m.ensureLeader(); err != nil {
		return nil, err
	}
	admin, ok := m.Store.(raftMembershipAdmin)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "raft membership admin not supported")
	}
	if err := admin.RemoveMaster(ctx, domain.NodeID(req.GetNodeId())); err != nil {
		return nil, mapErr(err)
	}
	return &godfsv1.RemoveMasterResponse{}, nil
}

func (m *MasterServer) ListChunkNodes(ctx context.Context, _ *godfsv1.ListChunkNodesRequest) (*godfsv1.ListChunkNodesResponse, error) {
	entries, err := usecase.ListChunkNodes(ctx, m.Store)
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]*godfsv1.ChunkNodeEntry, 0, len(entries))
	for i := range entries {
		e := entries[i]
		out = append(out, &godfsv1.ChunkNodeEntry{
			NodeId:        string(e.ID),
			GrpcAddress:   e.GRPCAddress,
			CapacityBytes: e.CapacityBytes,
			UsedBytes:     e.UsedBytes,
			LastSeenUnix:  e.LastSeenUnix,
			Alive:         e.Alive,
		})
	}
	return &godfsv1.ListChunkNodesResponse{Nodes: out}, nil
}

func (m *MasterServer) RunRebalanceNow(ctx context.Context, req *godfsv1.RunRebalanceNowRequest) (*godfsv1.RunRebalanceNowResponse, error) {
	if err := m.ensureLeader(); err != nil {
		return nil, err
	}
	ex, err := usecase.RunRebalanceNow(ctx, m.Store, int(req.GetMaxSteps()))
	if err != nil {
		return nil, mapErr(err)
	}
	return &godfsv1.RunRebalanceNowResponse{Executed: int32(ex)}, nil
}

func (m *MasterServer) ListUnderReplicatedChunks(ctx context.Context, req *godfsv1.ListUnderReplicatedChunksRequest) (*godfsv1.ListUnderReplicatedChunksResponse, error) {
	if err := m.ensureLeader(); err != nil {
		return nil, err
	}
	limit := int(req.GetLimit())
	entries, total, err := usecase.ListUnderReplicatedChunks(ctx, m.Store, limit)
	if err != nil {
		return nil, mapErr(err)
	}
	out := make([]*godfsv1.UnderReplicatedChunkEntry, 0, len(entries))
	for i := range entries {
		e := entries[i]
		out = append(out, &godfsv1.UnderReplicatedChunkEntry{
			ChunkId:           string(e.ChunkID),
			TargetReplication: int32(e.TargetReplication),
			AliveReplicas:     int32(e.AliveReplicas),
			TotalReplicas:     int32(e.TotalReplicas),
			SamplePaths:       append([]string(nil), e.SamplePaths...),
			DeadNodeIds:       append([]string(nil), e.DeadNodeIDs...),
		})
	}
	return &godfsv1.ListUnderReplicatedChunksResponse{
		Chunks:     out,
		TotalCount: int32(total),
	}, nil
}

func backupSnapshotToProto(m *domain.BackupSnapshot) *godfsv1.BackupManifest {
	if m == nil {
		return nil
	}
	files := make([]*godfsv1.BackupFileEntry, 0, len(m.Files))
	for _, f := range m.Files {
		chunks := make([]*godfsv1.BackupChunkRef, 0, len(f.Chunks))
		for _, c := range f.Chunks {
			reps := make([]*godfsv1.ReplicaLocation, len(c.Replicas))
			for i, r := range c.Replicas {
				reps[i] = &godfsv1.ReplicaLocation{NodeId: string(r.NodeID), GrpcAddress: r.Address}
			}
			chunks = append(chunks, &godfsv1.BackupChunkRef{
				ChunkId:        string(c.ChunkID),
				ChunkIndex:     c.ChunkIndex,
				Version:        c.Version,
				ChecksumSha256: append([]byte(nil), c.Checksum...),
				Replicas:       reps,
			})
		}
		files = append(files, &godfsv1.BackupFileEntry{
			Path:           f.Path,
			Size:           f.Size,
			Mode:           f.Mode,
			CreatedAtUnix:  f.CreatedAt.Unix(),
			ModifiedAtUnix: f.ModifiedAt.Unix(),
			Chunks:         chunks,
		})
	}
	return &godfsv1.BackupManifest{
		SnapshotId:        m.ID,
		Label:             m.Label,
		CreatedAtUnix:     m.CreatedAt.Unix(),
		ChunkSizeBytes:    m.ChunkSize,
		ReplicationFactor: int32(m.ReplicationFactor),
		Files:             files,
	}
}
