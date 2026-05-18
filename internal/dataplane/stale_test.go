package dataplane

import (
	"bytes"
	"testing"
)

func TestHasCommittedChunkChecksum(t *testing.T) {
	if HasCommittedChunkChecksum(nil) {
		t.Fatal("nil")
	}
	if HasCommittedChunkChecksum(make([]byte, 31)) {
		t.Fatal("short")
	}
	if !HasCommittedChunkChecksum(make([]byte, MetadataChunkChecksumBytes)) {
		t.Fatal("want true")
	}
}

func TestIsReplicaStaleComparedToMeta(t *testing.T) {
	meta := bytes.Repeat([]byte{1}, MetadataChunkChecksumBytes)
	good := append([]byte(nil), meta...)
	bad := append([]byte(nil), meta...)
	bad[0] ^= 0xff

	if IsReplicaStaleComparedToMeta(meta, good) {
		t.Fatal("identical should not be stale")
	}
	if !IsReplicaStaleComparedToMeta(meta, bad) {
		t.Fatal("expect stale")
	}
	if IsReplicaStaleComparedToMeta(meta[:16], bad) {
		t.Fatal("short meta")
	}
	if IsReplicaStaleComparedToMeta(nil, bad) {
		t.Fatal("nil meta")
	}
	zeros := make([]byte, MetadataChunkChecksumBytes)
	if !IsReplicaStaleComparedToMeta(meta, zeros) {
		t.Fatal("all-zero replica vs non-zero meta should be stale")
	}
	if IsReplicaStaleComparedToMeta(meta, meta[:31]) {
		t.Fatal("short replica digest")
	}
}
