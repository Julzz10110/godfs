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

// NewAuditLoggerFromEnv: GODFS_AUDIT_ENABLED=1 enables; GODFS_AUDIT_LOG=file path (append).
func NewAuditLoggerFromEnv() (*AuditLogger, error) {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("GODFS_AUDIT_ENABLED")))
	enabled := v == "1" || v == "true" || v == "yes"
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
	TS        string `json:"ts"`
	Principal string `json:"principal,omitempty"`
	Service   string `json:"service"`
	Method    string `json:"method"`
	Path      string `json:"path,omitempty"`
	OldPath   string `json:"old_path,omitempty"`
	NewPath   string `json:"new_path,omitempty"`
	OK        bool   `json:"ok"`
	Err       string `json:"err,omitempty"`
}

// LogMaster records a master RPC outcome.
func (a *AuditLogger) LogMaster(principal, method, path, oldPath, newPath string, ok bool, errMsg string) {
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
		OK:        ok,
		Err:       errMsg,
	}
	b, err := json.Marshal(ev)
	if err != nil {
		return
	}
	a.log.Println(string(b))
}
