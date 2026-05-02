package restgateway

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWithRequestID_propagatesHeader(t *testing.T) {
	var gotID string
	h := WithRequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotID = RequestIDFromContext(r.Context())
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	resp := rr.Result()
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)
	hdr := resp.Header.Get("X-Request-ID")
	if hdr == "" {
		t.Fatal("missing X-Request-ID response header")
	}
	if gotID != hdr {
		t.Fatalf("context id %q != header %q", gotID, hdr)
	}
}

func TestWithRequestID_respectsIncoming(t *testing.T) {
	var gotID string
	h := WithRequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotID = RequestIDFromContext(r.Context())
	}))

	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set("X-Request-ID", "abc-123")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if gotID != "abc-123" {
		t.Fatalf("got %q", gotID)
	}
	if rr.Header().Get("X-Request-ID") != "abc-123" {
		t.Fatalf("response header %q", rr.Header().Get("X-Request-ID"))
	}
}
