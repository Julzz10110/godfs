package restgateway

import (
	"errors"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"google.golang.org/grpc/status"
)

func (s *Server) handleMultipartInit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, errJSON{Error: "method not allowed"})
		return
	}
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
	id, err := s.multipart().Init(b.Path)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errJSON{Error: "multipart init failed"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"upload_id": id, "path": b.Path})
}

func (s *Server) handleMultipartUploadPart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		writeJSON(w, http.StatusMethodNotAllowed, errJSON{Error: "method not allowed"})
		return
	}
	uploadID := strings.TrimSpace(r.PathValue("uploadId"))
	if uploadID == "" {
		writeJSON(w, http.StatusBadRequest, errJSON{Error: "missing upload id"})
		return
	}
	pnStr := strings.TrimSpace(r.URL.Query().Get("partNumber"))
	pn, err := strconv.Atoi(pnStr)
	if err != nil || pn < 1 {
		writeJSON(w, http.StatusBadRequest, errJSON{Error: "invalid partNumber"})
		return
	}
	max := defaultMultipartMaxPartBytes()
	var body io.Reader = r.Body
	if max > 0 {
		body = http.MaxBytesReader(w, r.Body, max+1)
	}
	etag, _, err := s.multipart().PutPart(uploadID, pn, body, max)
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			out := errJSON{Error: "part too large", HTTPStatus: http.StatusRequestEntityTooLarge}
			if id := RequestIDFromContext(r.Context()); id != "" {
				out.RequestID = id
			}
			writeJSON(w, http.StatusRequestEntityTooLarge, out)
			return
		}
		if errors.Is(err, os.ErrNotExist) {
			writeJSON(w, http.StatusNotFound, errJSON{Error: "upload not found"})
			return
		}
		writeJSON(w, http.StatusBadRequest, errJSON{Error: err.Error()})
		return
	}
	w.Header().Set("ETag", `"`+etag+`"`)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleMultipartComplete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, errJSON{Error: "method not allowed"})
		return
	}
	uploadID := strings.TrimSpace(r.PathValue("uploadId"))
	if uploadID == "" {
		writeJSON(w, http.StatusBadRequest, errJSON{Error: "missing upload id"})
		return
	}
	var body multipartCompleteBody
	if !decodeJSONBody(w, r, &body) {
		return
	}
	ctx := OutgoingRPCContext(r)
	overwrite := r.URL.Query().Get("overwrite") == "1" || strings.EqualFold(r.URL.Query().Get("overwrite"), "true")
	path, err := s.multipart().Complete(ctx, s.Client, uploadID, body, overwrite)
	if err != nil {
		if errors.Is(err, errMultipartObjectExists) {
			writeJSON(w, http.StatusConflict, errJSON{Error: "object already exists", Code: "already_exists", HTTPStatus: http.StatusConflict})
			return
		}
		if errors.Is(err, errMultipartPathIsDir) {
			writeJSON(w, http.StatusBadRequest, errJSON{Error: err.Error()})
			return
		}
		if _, ok := status.FromError(err); ok {
			writeHTTPError(w, r, err)
			return
		}
		writeJSON(w, http.StatusBadRequest, errJSON{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"path": path})
}

func (s *Server) handleMultipartAbort(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		writeJSON(w, http.StatusMethodNotAllowed, errJSON{Error: "method not allowed"})
		return
	}
	uploadID := strings.TrimSpace(r.PathValue("uploadId"))
	if uploadID == "" {
		writeJSON(w, http.StatusBadRequest, errJSON{Error: "missing upload id"})
		return
	}
	s.multipart().Abort(uploadID)
	w.WriteHeader(http.StatusNoContent)
}
