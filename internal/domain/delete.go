package domain

// ChunkDeleteInfo describes where chunk bytes may still exist after metadata removal.
// It is used by the Master to issue best-effort deletes on chunk servers.
type ChunkDeleteInfo struct {
	ChunkID  ChunkID
	Replicas []string
}

