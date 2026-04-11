package config

import (
	"os"
	"strconv"

	"google.golang.org/grpc"
)

// defaultMaxGRPCMsgBytes is above DefaultChunkSize (64 MiB) to fit one chunk frame plus overhead.
const defaultMaxGRPCMsgBytes = 80 * 1024 * 1024

// MaxGRPCMsgBytes caps gRPC message size for chunk read/write streaming (client + server).
// Override with GODFS_GRPC_MAX_MSG_BYTES (must be ≥ chunk size used by the cluster).
func MaxGRPCMsgBytes() int {
	v := os.Getenv("GODFS_GRPC_MAX_MSG_BYTES")
	if v == "" {
		return defaultMaxGRPCMsgBytes
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return defaultMaxGRPCMsgBytes
	}
	return n
}

// GRPCServerOptions returns MaxRecvMsgSize / MaxSendMsgSize for ChunkService/MasterService.
func GRPCServerOptions() []grpc.ServerOption {
	n := MaxGRPCMsgBytes()
	return []grpc.ServerOption{
		grpc.MaxRecvMsgSize(n),
		grpc.MaxSendMsgSize(n),
	}
}

// GRPCDialOptions returns default call options so clients accept large chunk frames.
func GRPCDialOptions() []grpc.DialOption {
	n := MaxGRPCMsgBytes()
	return []grpc.DialOption{
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(n),
			grpc.MaxCallSendMsgSize(n),
		),
	}
}
