package security

import (
	"encoding/json"
	"log"
	"os"
	"strings"
	"time"
)

// AuditLogger writes one JSON object per line to GODFS_AUDIT_LOG or stdout when GODFS_AUDIT_LOG is empty but GODFS_AUDIT_ENABLED=1.
type AuditLogger struct {
	enabled bool
	log     *log.Logger
}

// AuditEnabledFromEnv reports whether GODFS_AUDIT_ENABLED is set.
func AuditEnabledFromEnv() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("GODFS_AUDIT_ENABLED")))
	return v == "1" || v == "true" || v == "yes"
}

// ChunkAuditEnabledFromEnv requires GODFS_AUDIT_ENABLED and GODFS_AUDIT_CHUNK for ChunkService audit lines.
func ChunkAuditEnabledFromEnv() bool {
	if !AuditEnabledFromEnv() {
		return false
	}
	v := strings.ToLower(strings.TrimSpace(os.Getenv("GODFS_AUDIT_CHUNK")))
	return v == "1" || v == "true" || v == "yes"
}

// NewAuditLoggerFromEnv: GODFS_AUDIT_ENABLED=1 enables; GODFS_AUDIT_LOG=file path (append).
func NewAuditLoggerFromEnv() (*AuditLogger, error) {
	enabled := AuditEnabledFromEnv()
	if !enabled {
		return &AuditLogger{enabled: false}, nil
	}
	path := strings.TrimSpace(os.Getenv("GODFS_AUDIT_LOG"))
	var w *os.File
	var err error
	if path == "" {
		w = os.Stdout
	} else {
		w, err = os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return nil, err
		}
	}
	return &AuditLogger{enabled: true, log: log.New(w, "", 0)}, nil
}

// Event is a single audit record.
type Event struct {
	TS         string `json:"ts"`
	Principal  string `json:"principal,omitempty"`
	Service    string `json:"service"`
	Method     string `json:"method"`
	Path       string `json:"path,omitempty"`
	OldPath    string `json:"old_path,omitempty"`
	NewPath    string `json:"new_path,omitempty"`
	ChunkID    string `json:"chunk_id,omitempty"`
	Streaming  bool   `json:"streaming,omitempty"`
	RequestID  string `json:"request_id,omitempty"`
	OK         bool   `json:"ok"`
	Err        string `json:"err,omitempty"`
}

// LogMaster records a master RPC outcome.
func (a *AuditLogger) LogMaster(principal, method, path, oldPath, newPath string, ok bool, errMsg, requestID string) {
	if a == nil || !a.enabled {
		return
	}
	ev := Event{
		TS:        time.Now().UTC().Format(time.RFC3339Nano),
		Principal: principal,
		Service:   "master",
		Method:    method,
		Path:      path,
		OldPath:   oldPath,
		NewPath:   newPath,
		RequestID: requestID,
		OK:        ok,
		Err:       errMsg,
	}
	b, err := json.Marshal(ev)
	if err != nil {
		return
	}
	a.log.Println(string(b))
}

// LogChunk records a ChunkService RPC outcome (principal is usually "cluster" when using GODFS_CLUSTER_KEY).
func (a *AuditLogger) LogChunk(principal, method, chunkID string, streaming bool, ok bool, errMsg, requestID string) {
	if a == nil || !a.enabled {
		return
	}
	ev := Event{
		TS:        time.Now().UTC().Format(time.RFC3339Nano),
		Principal: principal,
		Service:   "chunk",
		Method:    method,
		ChunkID:   chunkID,
		Streaming: streaming,
		RequestID: requestID,
		OK:        ok,
		Err:       errMsg,
	}
	b, err := json.Marshal(ev)
	if err != nil {
		return
	}
	a.log.Println(string(b))
}
