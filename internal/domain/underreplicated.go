package domain

// UnderReplicatedChunk describes a chunk with fewer live replicas than the target replication factor.
type UnderReplicatedChunk struct {
	ChunkID            ChunkID
	TargetReplication  int
	AliveReplicas      int
	TotalReplicas      int
	SamplePaths        []string
	DeadNodeIDs        []string
}
