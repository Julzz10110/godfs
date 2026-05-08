package domain

// MasterPeer describes one Raft master member (control plane).
type MasterPeer struct {
	NodeID      NodeID
	RaftAddress string
	GRPCAddress string
	Voter       bool
}

