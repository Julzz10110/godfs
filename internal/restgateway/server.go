package restgateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"

	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	godfsv1 "godfs/api/proto/godfs/v1"
	"godfs/pkg/client"
)

type gatewayClient interface {
	Mkdir(ctx context.Context, path string) error
	Create(ctx context.Context, path string) error
	Delete(ctx context.Context, path string) error
	Rename(ctx context.Context, oldPath, newPath string) error
	Stat(ctx context.Context, path string) (*client.FileInfo, error)
	List(ctx context.Context, path string) ([]*godfsv1.DirEntry, error)
	Read(ctx context.Context, path string) ([]byte, error)
	ReadRange(ctx context.Context, path string, offset, length int64) ([]byte, error)
	Write(ctx context.Context, path string, data []byte) error
	WriteFromReader(ctx context.Context, path string, r io.Reader) error

	CreateSnapshot(ctx context.Context, label string) (snapshotID string, createdAtUnix int64, err error)
	ListSnapshots(ctx context.Context) ([]*godfsv1.SnapshotListEntry, error)
	GetSnapshot(ctx context.Context, snapshotID string) (*godfsv1.BackupManifest, error)
	DeleteSnapshot(ctx context.Context, snapshotID string) error
}

// Server exposes a minimal REST mapping over pkg/client.
type Server struct {
	Client  gatewayClient
	MaxBody int64
}

type errJSON struct {
	Error      string `json:"error"`
	GRPCCode   string `json:"grpc_code,omitempty"`
	HTTPStatus int    `json:"http_status,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, err error) {
	st := statusFromGRPC(err)
	msg := err.Error()
	if st == http.StatusInternalServerError {
		msg = "internal error"
	}
	out := errJSON{
		Error:      msg,
		HTTPStatus: st,
	}
	if s, ok := status.FromError(err); ok {
		out.GRPCCode = s.Code().String()
	}
	writeJSON(w, st, out)
}

func requirePath(q string) (string, bool) {
	if q == "" || q[0] != '/' {
		return "", false
	}
	return q, true
}

// Register attaches /v1 routes to mux.
func (s *Server) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/health", s.handleHealth)
	mux.HandleFunc("GET /v1/fs/stat", s.handleStat)
	mux.HandleFunc("GET /v1/fs/list", s.handleList)
	mux.HandleFunc("POST /v1/fs/mkdir", s.handleMkdir)
	mux.HandleFunc("POST /v1/fs/file", s.handleCreateFile)
	mux.HandleFunc("DELETE /v1/fs", s.handleDelete)
	mux.HandleFunc("POST /v1/fs/rename", s.handleRename)
	mux.HandleFunc("GET /v1/fs/content", s.handleGetContent)
	mux.HandleFunc("PUT /v1/fs/content", s.handlePutContent)

	mux.HandleFunc("POST /v1/snapshots", s.handleCreateSnapshot)
	mux.HandleFunc("GET /v1/snapshots", s.handleListSnapshots)
	mux.HandleFunc("GET /v1/snapshots/{id}", s.handleGetSnapshot)
	mux.HandleFunc("DELETE /v1/snapshots/{id}", s.handleDeleteSnapshot)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleStat(w http.ResponseWriter, r *http.Request) {
	ctx := WithBearerAuth(r.Context(), r.Header.Get("Authorization"))
	p := r.URL.Query().Get("path")
	if okPath, ok := requirePath(p); !ok {
		writeJSON(w, http.StatusBadRequest, errJSON{Error: "invalid or missing path"})
		return
	} else {
		p = okPath
	}
	st, err := s.Client.Stat(ctx, p)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"path":             p,
		"is_dir":           st.IsDir,
		"size":             st.Size,
		"mode":             st.Mode,
		"modified_at_unix": st.ModTime.Unix(),
		"created_at_unix":  st.CreateAt.Unix(),
	})
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	ctx := WithBearerAuth(r.Context(), r.Header.Get("Authorization"))
	p := r.URL.Query().Get("path")
	if okPath, ok := requirePath(p); !ok {
		writeJSON(w, http.StatusBadRequest, errJSON{Error: "invalid or missing path"})
		return
	} else {
		p = okPath
	}
	entries, err := s.Client.List(ctx, p)
	if err != nil {
		writeErr(w, err)
		return
	}
	out := make([]map[string]interface{}, 0, len(entries))
	for _, e := range entries {
		out = append(out, map[string]interface{}{
			"name":   e.Name,
			"is_dir": e.IsDir,
			"size":   e.Size,
		})
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"entries": out})
}

type pathBody struct {
	Path string `json:"path"`
}

type renameBody struct {
	OldPath string `json:"old_path"`
	NewPath string `json:"new_path"`
}

func (s *Server) handleMkdir(w http.ResponseWriter, r *http.Request) {
	ctx := WithBearerAuth(r.Context(), r.Header.Get("Authorization"))
	var b pathBody
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		writeJSON(w, http.StatusBadRequest, errJSON{Error: "invalid json"})
		return
	}
	if okPath, ok := requirePath(b.Path); !ok {
		writeJSON(w, http.StatusBadRequest, errJSON{Error: "invalid path"})
		return
	} else {
		b.Path = okPath
	}
	if err := s.Client.Mkdir(ctx, b.Path); err != nil {
		writeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleCreateFile(w http.ResponseWriter, r *http.Request) {
	ctx := WithBearerAuth(r.Context(), r.Header.Get("Authorization"))
	var b pathBody
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		writeJSON(w, http.StatusBadRequest, errJSON{Error: "invalid json"})
		return
	}
	if okPath, ok := requirePath(b.Path); !ok {
		writeJSON(w, http.StatusBadRequest, errJSON{Error: "invalid path"})
		return
	} else {
		b.Path = okPath
	}
	if err := s.Client.Create(ctx, b.Path); err != nil {
		writeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	ctx := WithBearerAuth(r.Context(), r.Header.Get("Authorization"))
	p := r.URL.Query().Get("path")
	if okPath, ok := requirePath(p); !ok {
		writeJSON(w, http.StatusBadRequest, errJSON{Error: "invalid or missing path"})
		return
	} else {
		p = okPath
	}
	if err := s.Client.Delete(ctx, p); err != nil {
		writeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleRename(w http.ResponseWriter, r *http.Request) {
	ctx := WithBearerAuth(r.Context(), r.Header.Get("Authorization"))
	var b renameBody
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		writeJSON(w, http.StatusBadRequest, errJSON{Error: "invalid json"})
		return
	}
	o, ok1 := requirePath(b.OldPath)
	n, ok2 := requirePath(b.NewPath)
	if !ok1 || !ok2 {
		writeJSON(w, http.StatusBadRequest, errJSON{Error: "invalid paths"})
		return
	}
	if err := s.Client.Rename(ctx, o, n); err != nil {
		writeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleGetContent(w http.ResponseWriter, r *http.Request) {
	ctx := WithBearerAuth(r.Context(), r.Header.Get("Authorization"))
	p := r.URL.Query().Get("path")
	if okPath, ok := requirePath(p); !ok {
		writeJSON(w, http.StatusBadRequest, errJSON{Error: "invalid or missing path"})
		return
	} else {
		p = okPath
	}
	w.Header().Set("Accept-Ranges", "bytes")

	// Optional single-range support.
	if rh := strings.TrimSpace(r.Header.Get("Range")); rh != "" {
		st, err := s.Client.Stat(ctx, p)
		if err != nil {
			writeErr(w, err)
			return
		}
		if st.IsDir {
			writeJSON(w, http.StatusBadRequest, errJSON{Error: "is directory"})
			return
		}
		start, end, ok := parseSingleByteRange(rh, st.Size)
		if !ok {
			w.Header().Set("Content-Range", "bytes */"+strconv.FormatInt(st.Size, 10))
			writeJSON(w, http.StatusRequestedRangeNotSatisfiable, errJSON{Error: "invalid range"})
			return
		}
		data, err := s.Client.ReadRange(ctx, p, start, end-start)
		if err != nil {
			writeErr(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Range", "bytes "+strconv.FormatInt(start, 10)+"-"+strconv.FormatInt(end-1, 10)+"/"+strconv.FormatInt(st.Size, 10))
		w.Header().Set("Content-Length", strconv.FormatInt(int64(len(data)), 10))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data)
		return
	}

	data, err := s.Client.Read(ctx, p)
	if err != nil {
		writeErr(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (s *Server) handlePutContent(w http.ResponseWriter, r *http.Request) {
	ctx := WithBearerAuth(r.Context(), r.Header.Get("Authorization"))
	p := r.URL.Query().Get("path")
	if okPath, ok := requirePath(p); !ok {
		writeJSON(w, http.StatusBadRequest, errJSON{Error: "invalid or missing path"})
		return
	} else {
		p = okPath
	}
	max := s.MaxBody
	if max <= 0 {
		max = 80 << 20
	}
	r.Body = http.MaxBytesReader(w, r.Body, max+1)
	if err := s.Client.WriteFromReader(ctx, p, r.Body); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeJSON(w, http.StatusRequestEntityTooLarge, errJSON{Error: "body too large"})
			return
		}
		writeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type createSnapshotBody struct {
	Label string `json:"label"`
}

func (s *Server) handleCreateSnapshot(w http.ResponseWriter, r *http.Request) {
	ctx := WithBearerAuth(r.Context(), r.Header.Get("Authorization"))
	var b createSnapshotBody
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		writeJSON(w, http.StatusBadRequest, errJSON{Error: "invalid json"})
		return
	}
	id, ts, err := s.Client.CreateSnapshot(ctx, b.Label)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"snapshot_id":      id,
		"created_at_unix":  ts,
	})
}

func (s *Server) handleListSnapshots(w http.ResponseWriter, r *http.Request) {
	ctx := WithBearerAuth(r.Context(), r.Header.Get("Authorization"))
	entries, err := s.Client.ListSnapshots(ctx)
	if err != nil {
		writeErr(w, err)
		return
	}
	out := make([]map[string]interface{}, 0, len(entries))
	for _, e := range entries {
		out = append(out, map[string]interface{}{
			"snapshot_id":     e.GetSnapshotId(),
			"label":           e.GetLabel(),
			"created_at_unix": e.GetCreatedAtUnix(),
			"file_count":      e.GetFileCount(),
		})
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"snapshots": out})
}

func (s *Server) handleGetSnapshot(w http.ResponseWriter, r *http.Request) {
	ctx := WithBearerAuth(r.Context(), r.Header.Get("Authorization"))
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		writeJSON(w, http.StatusBadRequest, errJSON{Error: "missing snapshot id"})
		return
	}
	manifest, err := s.Client.GetSnapshot(ctx, id)
	if err != nil {
		writeErr(w, err)
		return
	}
	b, err := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(manifest)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errJSON{Error: "internal error"})
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(b)
}

func (s *Server) handleDeleteSnapshot(w http.ResponseWriter, r *http.Request) {
	ctx := WithBearerAuth(r.Context(), r.Header.Get("Authorization"))
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		writeJSON(w, http.StatusBadRequest, errJSON{Error: "missing snapshot id"})
		return
	}
	if err := s.Client.DeleteSnapshot(ctx, id); err != nil {
		writeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// DefaultMaxBodyBytes returns the limit for PUT bodies from GODFS_REST_MAX_BODY_BYTES or default 80 MiB.
func DefaultMaxBodyBytes() int64 {
	const def = 80 << 20
	s := strings.TrimSpace(os.Getenv("GODFS_REST_MAX_BODY_BYTES"))
	if s == "" {
		return def
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 {
		return def
	}
	return n
}

var singleRangeRe = regexp.MustCompile(`^bytes=(\d+)-(\d+)?$`)

// parseSingleByteRange parses a single RFC 7233 byte range header for known size.
// It returns [start,end) if valid and satisfiable.
func parseSingleByteRange(h string, size int64) (start, end int64, ok bool) {
	if size < 0 {
		return 0, 0, false
	}
	m := singleRangeRe.FindStringSubmatch(strings.TrimSpace(h))
	if m == nil {
		return 0, 0, false
	}
	s, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil || s < 0 {
		return 0, 0, false
	}
	var e int64
	if m[2] == "" {
		e = size
	} else {
		v, err := strconv.ParseInt(m[2], 10, 64)
		if err != nil || v < 0 {
			return 0, 0, false
		}
		e = v + 1
	}
	if s >= size {
		return 0, 0, false
	}
	if e > size {
		e = size
	}
	if e <= s {
		return 0, 0, false
	}
	return s, e, true
}
