package domain

import "errors"

var (
	ErrNotFound         = errors.New("not found")
	ErrAlreadyExists    = errors.New("already exists")
	ErrNotEmpty         = errors.New("directory not empty")
	ErrIsDir            = errors.New("is a directory")
	ErrNotDir           = errors.New("not a directory")
	ErrInvalidPath      = errors.New("invalid path")
	ErrNoChunkServer    = errors.New("no chunk server registered")
	ErrLeaseConflict    = errors.New("lease conflict")
	ErrChunkMismatch    = errors.New("chunk version mismatch")
	ErrParentNotFound   = errors.New("parent directory not found")
	ErrInsufficientChunkServers = errors.New("insufficient chunk servers for replication")
	// ErrNotLeader means the request must be sent to the current Raft leader (metadata mutations).
	ErrNotLeader = errors.New("not leader")
)
