package security

import "testing"

func TestParseGRPCPeerStreamLimitEnv_Disabled(t *testing.T) {
	t.Setenv("GODFS_GRPC_PEER_STREAM_MAX_CONCURRENT", "")
	if parseGRPCPeerStreamLimitEnv() != 0 {
		t.Fatal("expected disabled")
	}
}

func TestParseGRPCPeerStreamLimitEnv_Enabled(t *testing.T) {
	t.Setenv("GODFS_GRPC_PEER_STREAM_MAX_CONCURRENT", "8")
	if parseGRPCPeerStreamLimitEnv() != 8 {
		t.Fatal("expected 8")
	}
}

func TestPeerStreamLimiter_IndependentPeers(t *testing.T) {
	pl := &peerStreamLimiter{max: 1, active: make(map[string]int)}
	if err := pl.acquire("a"); err != nil {
		t.Fatal(err)
	}
	if err := pl.acquire("a"); err == nil {
		t.Fatal("expected limit for peer a")
	}
	if err := pl.acquire("b"); err != nil {
		t.Fatalf("peer b: %v", err)
	}
	pl.release("a")
	if err := pl.acquire("a"); err != nil {
		t.Fatalf("after release: %v", err)
	}
}

func TestGRPCStreamPeerRateLimitFromEnv_NilWhenUnset(t *testing.T) {
	t.Setenv("GODFS_GRPC_PEER_STREAM_MAX_CONCURRENT", "")
	if GRPCStreamPeerRateLimitFromEnv() != nil {
		t.Fatal("expected nil interceptor")
	}
}

func TestSubjectToGRPCPeerStreamLimit_ChunkStreamsOnly(t *testing.T) {
	if !subjectToGRPCPeerStreamLimit("/godfs.v1.ChunkService/ReadChunk") {
		t.Fatal("ReadChunk should be limited")
	}
	if !subjectToGRPCPeerStreamLimit("/godfs.v1.ChunkService/PullChunk") {
		t.Fatal("PullChunk should be limited")
	}
	if subjectToGRPCPeerStreamLimit("/godfs.v1.MasterService/Heartbeat") {
		t.Fatal("Heartbeat must not be limited")
	}
}
