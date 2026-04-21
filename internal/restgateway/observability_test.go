package restgateway

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWithHTTPMetrics_DoesNotPanicAndServes(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/health", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})

	h := WithHTTPMetrics(mux)
	// Wrap twice to ensure registration/path is stable.
	h = WithHTTPMetrics(h)

	req := httptest.NewRequest("GET", "http://example/v1/health", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	resp := rr.Result()
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d body=%q", resp.StatusCode, string(b))
	}
}

