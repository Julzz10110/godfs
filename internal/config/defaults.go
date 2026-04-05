package config

const (
	DefaultChunkSize   = 64 * 1024 * 1024 // 64 MiB
	DefaultLeaseSec    = 60
	DefaultReplication = 3 // target replica count (requires >= N registered chunk servers)
)
