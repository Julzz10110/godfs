package restgateway

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"godfs/pkg/client"
)

// Server exposes a minimal REST mapping over pkg/client.
type Server struct {
	Client  *client.Client
	MaxBody int64
}

type errJSON struct {
	Error string `json:"error"`
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
	writeJSON(w, st, errJSON{Error: msg})
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
	data, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusRequestEntityTooLarge, errJSON{Error: "body too large"})
		return
	}
	if err := s.Client.Write(ctx, p, data); err != nil {
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
