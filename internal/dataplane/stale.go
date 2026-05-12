package dataplane

import "bytes"

// MetadataChunkChecksumBytes is the length of SHA-256 stored on the master after commit.
const MetadataChunkChecksumBytes = 32

// HasCommittedChunkChecksum reports whether metadata carries a full-chunk checksum
// suitable for replica health / stale detection (Production-2 repair policy).
func HasCommittedChunkChecksum(sum []byte) bool {
	return len(sum) == MetadataChunkChecksumBytes
}

// IsReplicaStaleComparedToMeta returns true when both sums are 32-byte SHA-256 digests
// and the replica digest differs from the metadata digest.
//
// Repair policy (goDFS Production-2): when this returns true for a live replica and
// another live replica matches metadata, the rebalancer overwrites this replica via
// PullChunk from a good source (see PlanRebalance). The authoritative value is the
// checksum committed in metadata after a successful write, not "primary wins" alone.
func IsReplicaStaleComparedToMeta(metadataSum, replicaSum []byte) bool {
	if !HasCommittedChunkChecksum(metadataSum) || !HasCommittedChunkChecksum(replicaSum) {
		return false
	}
	return !bytes.Equal(metadataSum, replicaSum)
}
