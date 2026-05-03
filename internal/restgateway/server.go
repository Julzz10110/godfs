package restgateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	godfsv1 "godfs/api/proto/godfs/v1"
	"godfs/internal/usecase"
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
	StreamRangeToWriter(ctx context.Context, path string, offset, length, segment int64, w io.Writer) (written int64, err error)
	Write(ctx context.Context, path string, data []byte) error
	WriteFromReader(ctx context.Context, path string, r io.Reader) error

	CreateSnapshot(ctx context.Context, label string) (snapshotID string, createdAtUnix int64, err error)
	ListSnapshots(ctx context.Context) ([]*godfsv1.SnapshotListEntry, error)
	GetSnapshot(ctx context.Context, snapshotID string) (*godfsv1.BackupManifest, error)
	DeleteSnapshot(ctx context.Context, snapshotID string) error
}

// NoMaxUploadLimit disables the byte cap on PUT /v1/fs/content (streaming; client buffers at most one chunk).
const NoMaxUploadLimit int64 = -1

// Server exposes a minimal REST mapping over pkg/client.
type Server struct {
	Client gatewayClient
	// MaxUpload caps total bytes read for PUT /v1/fs/content (via MaxBytesReader). Zero means use [DefaultMaxUploadBytes].
	// [NoMaxUploadLimit] disables the cap.
	MaxUpload int64
	// StreamSegment is max bytes per internal read when streaming GET / Range bodies. Zero means [DefaultGetStreamBytes].
	StreamSegment int64

	mpOnce sync.Once
	mpart  *multipartManager
}

func (s *Server) multipart() *multipartManager {
	s.mpOnce.Do(func() {
		s.mpart = newMultipartManager(DefaultMultipartDataDir())
	})
	return s.mpart
}

func (s *Server) putUploadLimit() int64 {
	if s.MaxUpload == NoMaxUploadLimit {
		return 0
	}
	if s.MaxUpload > 0 {
		return s.MaxUpload
	}
	return DefaultMaxUploadBytes()
}

func (s *Server) streamSegment() int64 {
	if s.StreamSegment > 0 {
		return s.StreamSegment
	}
	return DefaultGetStreamBytes()
}

type errJSON struct {
	Error      string `json:"error"`
	Code       string `json:"code,omitempty"`
	GRPCCode   string `json:"grpc_code,omitempty"`
	HTTPStatus int    `json:"http_status,omitempty"`
	RequestID  string `json:"request_id,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeHTTPError(w http.ResponseWriter, r *http.Request, err error) {
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
		out.Code = grpcCodeToRESTCode(s.Code().String())
	}
	if id := RequestIDFromContext(r.Context()); id != "" {
		out.RequestID = id
	}
	writeJSON(w, st, out)
}

func decodeJSONBody(w http.ResponseWriter, r *http.Request, dst interface{}) bool {
	max := DefaultMaxJSONBodyBytes()
	r.Body = http.MaxBytesReader(w, r.Body, max+1)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(dst); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			out := errJSON{Error: "json body too large", HTTPStatus: http.StatusRequestEntityTooLarge}
			if id := RequestIDFromContext(r.Context()); id != "" {
				out.RequestID = id
			}
			writeJSON(w, http.StatusRequestEntityTooLarge, out)
			return false
		}
		out := errJSON{Error: "invalid json", HTTPStatus: http.StatusBadRequest}
		if id := RequestIDFromContext(r.Context()); id != "" {
			out.RequestID = id
		}
		writeJSON(w, http.StatusBadRequest, out)
		return false
	}
	return true
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
	mux.HandleFunc("HEAD /v1/fs/content", s.handleHeadContent)
	mux.HandleFunc("PUT /v1/fs/content", s.handlePutContent)

	mux.HandleFunc("POST /v1/fs/multipart", s.handleMultipartInit)
	mux.HandleFunc("PUT /v1/fs/multipart/{uploadId}", s.handleMultipartUploadPart)
	mux.HandleFunc("POST /v1/fs/multipart/{uploadId}/complete", s.handleMultipartComplete)
	mux.HandleFunc("DELETE /v1/fs/multipart/{uploadId}", s.handleMultipartAbort)

	mux.HandleFunc("POST /v1/snapshots", s.handleCreateSnapshot)
	mux.HandleFunc("GET /v1/snapshots", s.handleListSnapshots)
	mux.HandleFunc("GET /v1/snapshots/{id}", s.handleGetSnapshot)
	mux.HandleFunc("DELETE /v1/snapshots/{id}", s.handleDeleteSnapshot)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleStat(w http.ResponseWriter, r *http.Request) {
	ctx := OutgoingRPCContext(r)
	p := r.URL.Query().Get("path")
	if okPath, ok := requirePath(p); !ok {
		writeJSON(w, http.StatusBadRequest, errJSON{Error: "invalid or missing path"})
		return
	} else {
		p = okPath
	}
	st, err := s.Client.Stat(ctx, p)
	if err != nil {
		writeHTTPError(w, r, err)
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
	ctx := OutgoingRPCContext(r)
	p := r.URL.Query().Get("path")
	if okPath, ok := requirePath(p); !ok {
		writeJSON(w, http.StatusBadRequest, errJSON{Error: "invalid or missing path"})
		return
	} else {
		p = okPath
	}
	entries, err := s.Client.List(ctx, p)
	if err != nil {
		writeHTTPError(w, r, err)
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
	ctx := OutgoingRPCContext(r)
	var b pathBody
	if !decodeJSONBody(w, r, &b) {
		return
	}
	if okPath, ok := requirePath(b.Path); !ok {
		writeJSON(w, http.StatusBadRequest, errJSON{Error: "invalid path"})
		return
	} else {
		b.Path = okPath
	}
	if err := s.Client.Mkdir(ctx, b.Path); err != nil {
		writeHTTPError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleCreateFile(w http.ResponseWriter, r *http.Request) {
	ctx := OutgoingRPCContext(r)
	var b pathBody
	if !decodeJSONBody(w, r, &b) {
		return
	}
	if okPath, ok := requirePath(b.Path); !ok {
		writeJSON(w, http.StatusBadRequest, errJSON{Error: "invalid path"})
		return
	} else {
		b.Path = okPath
	}
	if err := s.Client.Create(ctx, b.Path); err != nil {
		writeHTTPError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	ctx := OutgoingRPCContext(r)
	p := r.URL.Query().Get("path")
	if okPath, ok := requirePath(p); !ok {
		writeJSON(w, http.StatusBadRequest, errJSON{Error: "invalid or missing path"})
		return
	} else {
		p = okPath
	}
	if err := s.Client.Delete(ctx, p); err != nil {
		writeHTTPError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleRename(w http.ResponseWriter, r *http.Request) {
	ctx := OutgoingRPCContext(r)
	var b renameBody
	if !decodeJSONBody(w, r, &b) {
		return
	}
	o, ok1 := requirePath(b.OldPath)
	n, ok2 := requirePath(b.NewPath)
	if !ok1 || !ok2 {
		writeJSON(w, http.StatusBadRequest, errJSON{Error: "invalid paths"})
		return
	}
	if err := s.Client.Rename(ctx, o, n); err != nil {
		writeHTTPError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleGetContent(w http.ResponseWriter, r *http.Request) {
	s.handleGetOrHeadContent(w, r, false)
}

func (s *Server) handleHeadContent(w http.ResponseWriter, r *http.Request) {
	s.handleGetOrHeadContent(w, r, true)
}

func (s *Server) handleGetOrHeadContent(w http.ResponseWriter, r *http.Request, headOnly bool) {
	ctx := OutgoingRPCContext(r)
	p := r.URL.Query().Get("path")
	if okPath, ok := requirePath(p); !ok {
		writeJSON(w, http.StatusBadRequest, errJSON{Error: "invalid or missing path"})
		return
	} else {
		p = okPath
	}
	w.Header().Set("Accept-Ranges", "bytes")

	// Stat first to attach validators and support conditional requests without reading body.
	st, err := s.Client.Stat(ctx, p)
	if err != nil {
		writeHTTPError(w, r, err)
		return
	}
	if st.IsDir {
		writeJSON(w, http.StatusBadRequest, errJSON{Error: "is directory"})
		return
	}
	etag := contentETag(st)
	w.Header().Set("ETag", etag)
	w.Header().Set("Last-Modified", st.ModTime.UTC().Format(http.TimeFormat))

	// Conditional requests (RFC 9110): If-None-Match / If-Modified-Since => 304 for GET/HEAD.
	if isNotModified(r, etag, st.ModTime) {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	// Optional Range support (single + multi-range).
	if rh := strings.TrimSpace(r.Header.Get("Range")); rh != "" {
		// If-Range: if it doesn't match current validator, ignore Range and return full response.
		if ir := strings.TrimSpace(r.Header.Get("If-Range")); ir != "" && !ifRangeMatches(ir, etag, st.ModTime) {
			rh = ""
		}

		if rh != "" {
			ranges, ok := parseMultiByteRanges(rh, st.Size, 16)
			if !ok || len(ranges) == 0 {
				w.Header().Set("Content-Range", "bytes */"+strconv.FormatInt(st.Size, 10))
				writeJSON(w, http.StatusRequestedRangeNotSatisfiable, errJSON{Error: "invalid range"})
				return
			}

			// Single-range fast path (streamed; bounded by StreamSegment memory per read).
			if len(ranges) == 1 {
				start, end := ranges[0].start, ranges[0].end
				ln := end - start
				w.Header().Set("Content-Type", "application/octet-stream")
				w.Header().Set("Content-Range", "bytes "+strconv.FormatInt(start, 10)+"-"+strconv.FormatInt(end-1, 10)+"/"+strconv.FormatInt(st.Size, 10))
				w.Header().Set("Content-Length", strconv.FormatInt(ln, 10))
				w.WriteHeader(http.StatusPartialContent)
				if !headOnly {
					if _, err := s.Client.StreamRangeToWriter(ctx, p, start, ln, s.streamSegment(), w); err != nil {
						return
					}
				}
				return
			}

			// Multi-range: multipart/byteranges.
			boundary := "godfs-" + strconv.FormatUint(rand.Uint64(), 16)
			w.Header().Set("Content-Type", `multipart/byteranges; boundary=`+boundary)
			w.WriteHeader(http.StatusPartialContent)
			if headOnly {
				return
			}
			for _, rg := range ranges {
				if err := writeMultipartRangePart(w, boundary, st.Size, rg.start, rg.end); err != nil {
					// best-effort; connection likely broken
					return
				}
				if _, err := s.Client.StreamRangeToWriter(ctx, p, rg.start, rg.end-rg.start, s.streamSegment(), w); err != nil {
					return
				}
				if _, err := w.Write([]byte("\r\n")); err != nil {
					return
				}
			}
			_, _ = w.Write([]byte("--" + boundary + "--\r\n"))
			return
		}
	}

	// Full-body response (no Range, or If-Range mismatch) — streamed without buffering whole file.
	if headOnly {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.FormatInt(st.Size, 10))
		w.WriteHeader(http.StatusOK)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(st.Size, 10))
	w.WriteHeader(http.StatusOK)
	if st.Size > 0 {
		if _, err := s.Client.StreamRangeToWriter(ctx, p, 0, st.Size, s.streamSegment(), w); err != nil {
			return
		}
	}
}

func (s *Server) handlePutContent(w http.ResponseWriter, r *http.Request) {
	ctx := OutgoingRPCContext(r)
	p := r.URL.Query().Get("path")
	if okPath, ok := requirePath(p); !ok {
		writeJSON(w, http.StatusBadRequest, errJSON{Error: "invalid or missing path"})
		return
	} else {
		p = okPath
	}
	max := s.putUploadLimit()
	if max > 0 {
		r.Body = http.MaxBytesReader(w, r.Body, max+1)
	}
	if err := s.Client.WriteFromReader(ctx, p, r.Body); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			out := errJSON{Error: "body too large", HTTPStatus: http.StatusRequestEntityTooLarge}
			if id := RequestIDFromContext(r.Context()); id != "" {
				out.RequestID = id
			}
			writeJSON(w, http.StatusRequestEntityTooLarge, out)
			return
		}
		writeHTTPError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type createSnapshotBody struct {
	Label string `json:"label"`
}

func (s *Server) handleCreateSnapshot(w http.ResponseWriter, r *http.Request) {
	ctx := OutgoingRPCContext(r)
	var b createSnapshotBody
	if !decodeJSONBody(w, r, &b) {
		return
	}
	if err := usecase.ValidateSnapshotLabel(b.Label); err != nil {
		writeJSON(w, http.StatusBadRequest, errJSON{Error: err.Error()})
		return
	}
	id, ts, err := s.Client.CreateSnapshot(ctx, b.Label)
	if err != nil {
		writeHTTPError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"snapshot_id":     id,
		"created_at_unix": ts,
	})
}

func (s *Server) handleListSnapshots(w http.ResponseWriter, r *http.Request) {
	ctx := OutgoingRPCContext(r)
	entries, err := s.Client.ListSnapshots(ctx)
	if err != nil {
		writeHTTPError(w, r, err)
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
	ctx := OutgoingRPCContext(r)
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		writeJSON(w, http.StatusBadRequest, errJSON{Error: "missing snapshot id"})
		return
	}
	manifest, err := s.Client.GetSnapshot(ctx, id)
	if err != nil {
		writeHTTPError(w, r, err)
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
	ctx := OutgoingRPCContext(r)
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		writeJSON(w, http.StatusBadRequest, errJSON{Error: "missing snapshot id"})
		return
	}
	if err := s.Client.DeleteSnapshot(ctx, id); err != nil {
		writeHTTPError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// DefaultMaxBodyBytes returns the limit for small JSON bodies from GODFS_REST_MAX_BODY_BYTES or default 80 MiB.
// PUT /v1/fs/content uses [DefaultMaxUploadBytes] unless [Server.MaxUpload] overrides.
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

// DefaultMaxUploadBytes caps total bytes read for PUT /v1/fs/content.
// If GODFS_REST_MAX_UPLOAD_BYTES is not set, falls back to [DefaultMaxBodyBytes] (backward compatible).
// If set to 0 or a negative number, returns 0 (no cap — streaming; memory bounded by one chunk in pkg/client).
func DefaultMaxUploadBytes() int64 {
	v, ok := os.LookupEnv("GODFS_REST_MAX_UPLOAD_BYTES")
	if !ok {
		return DefaultMaxBodyBytes()
	}
	v = strings.TrimSpace(v)
	if v == "" {
		return DefaultMaxBodyBytes()
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return DefaultMaxBodyBytes()
	}
	if n <= 0 {
		return 0
	}
	return n
}

var singleRangeRe = regexp.MustCompile(`^bytes=(\d+)-(\d+)?$`)
var suffixRangeRe = regexp.MustCompile(`^bytes=-(\d+)$`)

type byteRange struct {
	start int64
	end   int64 // exclusive
}

// parseSingleByteRange parses a single RFC 7233 byte range header for known size.
// Supported forms:
//   - bytes=start-end
//   - bytes=start-
//   - bytes=-N (suffix range: last N bytes)
//
// It returns [start,end) if valid and satisfiable.
func parseSingleByteRange(h string, size int64) (start, end int64, ok bool) {
	if size < 0 {
		return 0, 0, false
	}
	h = strings.TrimSpace(h)

	// Suffix: bytes=-N (last N bytes)
	if m := suffixRangeRe.FindStringSubmatch(h); m != nil {
		if size == 0 {
			return 0, 0, false
		}
		n, err := strconv.ParseInt(m[1], 10, 64)
		if err != nil || n <= 0 {
			return 0, 0, false
		}
		if n >= size {
			return 0, size, true
		}
		return size - n, size, true
	}

	// Regular: bytes=start-end | bytes=start-
	m := singleRangeRe.FindStringSubmatch(h)
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

func parseMultiByteRanges(h string, size int64, maxRanges int) ([]byteRange, bool) {
	h = strings.TrimSpace(h)
	if !strings.HasPrefix(strings.ToLower(h), "bytes=") {
		return nil, false
	}
	spec := strings.TrimSpace(h[len("bytes="):])
	if spec == "" {
		return nil, false
	}
	parts := strings.Split(spec, ",")
	if maxRanges <= 0 {
		maxRanges = 16
	}
	if len(parts) > maxRanges {
		return nil, false
	}
	out := make([]byteRange, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			return nil, false
		}
		s, e, ok := parseSingleByteRange("bytes="+p, size)
		if !ok {
			return nil, false
		}
		out = append(out, byteRange{start: s, end: e})
	}
	return out, true
}

func contentETag(st *client.FileInfo) string {
	// Strong ETag based on size and mtime seconds. Good enough for gateway caching/If-Range.
	// Format: "sizeHex-mtimeHex"
	return `"` + strconv.FormatInt(st.Size, 16) + "-" + strconv.FormatInt(st.ModTime.Unix(), 16) + `"`
}

func ifRangeMatches(ifRangeHeader, etag string, modTime time.Time) bool {
	v := strings.TrimSpace(ifRangeHeader)
	if v == "" {
		return true
	}
	// ETag case (quoted or weak).
	if strings.HasPrefix(v, `"`) || strings.HasPrefix(strings.ToLower(v), "w/") {
		return strings.EqualFold(v, etag)
	}
	// HTTP-date case.
	if t, err := http.ParseTime(v); err == nil {
		// If the resource has been modified after this date, ranges must be ignored.
		return !modTime.After(t)
	}
	// Unknown validator format -> be conservative: ignore Range.
	return false
}

func writeMultipartRangePart(w io.Writer, boundary string, size, start, end int64) error {
	// Per RFC 7233, each part contains Content-Type and Content-Range.
	_, err := io.WriteString(w, "--"+boundary+"\r\n"+
		"Content-Type: application/octet-stream\r\n"+
		"Content-Range: bytes "+strconv.FormatInt(start, 10)+"-"+strconv.FormatInt(end-1, 10)+"/"+strconv.FormatInt(size, 10)+"\r\n"+
		"\r\n")
	return err
}

func isNotModified(r *http.Request, etag string, modTime time.Time) bool {
	if inm := strings.TrimSpace(r.Header.Get("If-None-Match")); inm != "" {
		if ifNoneMatchMatches(inm, etag) {
			return true
		}
	}
	if ims := strings.TrimSpace(r.Header.Get("If-Modified-Since")); ims != "" {
		if t, err := http.ParseTime(ims); err == nil {
			// Not modified since date => 304.
			if !modTime.After(t) {
				return true
			}
		}
	}
	return false
}

func ifNoneMatchMatches(headerVal, etag string) bool {
	v := strings.TrimSpace(headerVal)
	if v == "" {
		return false
	}
	if v == "*" {
		return true
	}
	// Comma-separated list of ETags.
	for _, part := range strings.Split(v, ",") {
		p := strings.TrimSpace(part)
		if p == "" {
			continue
		}
		if strings.EqualFold(p, etag) {
			return true
		}
		// Weak comparison: allow If-None-Match: W/"..."
		if strings.HasPrefix(strings.ToLower(p), "w/") && strings.EqualFold(strings.TrimSpace(p[2:]), etag) {
			return true
		}
	}
	return false
}
