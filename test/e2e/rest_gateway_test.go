package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"godfs/internal/restgateway"
	"godfs/pkg/client"
	"godfs/test/e2e"
)

func TestE2E_RESTGateway_PutGetRangeAndSnapshots(t *testing.T) {
	const chunkSize = 256 * 1024
	_, cl := e2e.StartMaster(t, chunkSize, 1)
	dir := t.TempDir()
	cl.AddChunkServer(t, "chunk-a", filepath.Join(dir, "c0"))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	gwCli, err := client.NewGateway(cl.MasterAddr, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	defer gwCli.Close()

	srv := &restgateway.Server{Client: gwCli, MaxBody: 10 << 20}
	mux := http.NewServeMux()
	srv.Register(mux)
	httpSrv := httptest.NewServer(mux)
	defer httpSrv.Close()

	doJSON := func(method, path string, body any) *http.Response {
		t.Helper()
		var r io.Reader
		if body != nil {
			b, err := json.Marshal(body)
			if err != nil {
				t.Fatalf("json marshal: %v", err)
			}
			r = bytes.NewReader(b)
		}
		req, err := http.NewRequestWithContext(ctx, method, httpSrv.URL+path, r)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("do: %v", err)
		}
		return resp
	}

	mustNoContent := func(resp *http.Response) {
		t.Helper()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			b, _ := io.ReadAll(resp.Body)
			t.Fatalf("status=%d body=%q", resp.StatusCode, string(b))
		}
	}

	mustOK := func(resp *http.Response) []byte {
		t.Helper()
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status=%d body=%q", resp.StatusCode, string(b))
		}
		return b
	}

	mustNoContent(doJSON("POST", "/v1/fs/mkdir", map[string]string{"path": "/rg"}))
	mustNoContent(doJSON("POST", "/v1/fs/file", map[string]string{"path": "/rg/a.bin"}))

	payload := make([]byte, 3*chunkSize+123) // cross chunk boundaries
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	putReq, err := http.NewRequestWithContext(ctx, "PUT", httpSrv.URL+"/v1/fs/content?path=/rg/a.bin", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	putResp, err := http.DefaultClient.Do(putReq)
	if err != nil {
		t.Fatal(err)
	}
	defer putResp.Body.Close()
	if putResp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(putResp.Body)
		t.Fatalf("put status=%d body=%q", putResp.StatusCode, string(b))
	}

	getResp, err := http.NewRequestWithContext(ctx, "GET", httpSrv.URL+"/v1/fs/content?path=/rg/a.bin", nil)
	if err != nil {
		t.Fatal(err)
	}
	gotResp, err := http.DefaultClient.Do(getResp)
	if err != nil {
		t.Fatal(err)
	}
	got := mustOK(gotResp)
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch: got=%d want=%d", len(got), len(payload))
	}

	// Range: bytes=10-19 (10 bytes)
	rangeReq, err := http.NewRequestWithContext(ctx, "GET", httpSrv.URL+"/v1/fs/content?path=/rg/a.bin", nil)
	if err != nil {
		t.Fatal(err)
	}
	rangeReq.Header.Set("Range", "bytes=10-19")
	rangeResp, err := http.DefaultClient.Do(rangeReq)
	if err != nil {
		t.Fatal(err)
	}
	defer rangeResp.Body.Close()
	rb, _ := io.ReadAll(rangeResp.Body)
	if rangeResp.StatusCode != http.StatusPartialContent {
		t.Fatalf("range status=%d body=%q", rangeResp.StatusCode, string(rb))
	}
	if len(rb) != 10 {
		t.Fatalf("range len=%d want 10", len(rb))
	}
	if !bytes.Equal(rb, payload[10:20]) {
		t.Fatalf("range mismatch")
	}
	if cr := rangeResp.Header.Get("Content-Range"); cr == "" {
		t.Fatalf("missing Content-Range")
	}

	// Suffix range: bytes=-10 (last 10 bytes)
	suffixReq, err := http.NewRequestWithContext(ctx, "GET", httpSrv.URL+"/v1/fs/content?path=/rg/a.bin", nil)
	if err != nil {
		t.Fatal(err)
	}
	suffixReq.Header.Set("Range", "bytes=-10")
	suffixResp, err := http.DefaultClient.Do(suffixReq)
	if err != nil {
		t.Fatal(err)
	}
	defer suffixResp.Body.Close()
	sb, _ := io.ReadAll(suffixResp.Body)
	if suffixResp.StatusCode != http.StatusPartialContent {
		t.Fatalf("suffix status=%d body=%q", suffixResp.StatusCode, string(sb))
	}
	if len(sb) != 10 {
		t.Fatalf("suffix len=%d want 10", len(sb))
	}
	if !bytes.Equal(sb, payload[len(payload)-10:]) {
		t.Fatalf("suffix mismatch")
	}

	// Multi-range: bytes=0-0,2-3 (multipart/byteranges)
	mrReq, err := http.NewRequestWithContext(ctx, "GET", httpSrv.URL+"/v1/fs/content?path=/rg/a.bin", nil)
	if err != nil {
		t.Fatal(err)
	}
	mrReq.Header.Set("Range", "bytes=0-0,2-3")
	mrResp, err := http.DefaultClient.Do(mrReq)
	if err != nil {
		t.Fatal(err)
	}
	defer mrResp.Body.Close()
	if mrResp.StatusCode != http.StatusPartialContent {
		b, _ := io.ReadAll(mrResp.Body)
		t.Fatalf("multi-range status=%d body=%q", mrResp.StatusCode, string(b))
	}
	mediatype, params, err := mime.ParseMediaType(mrResp.Header.Get("Content-Type"))
	if err != nil {
		t.Fatalf("parse media type: %v", err)
	}
	if mediatype != "multipart/byteranges" {
		t.Fatalf("mediatype=%q", mediatype)
	}
	boundary := params["boundary"]
	if boundary == "" {
		t.Fatal("missing boundary")
	}
	rawMR, _ := io.ReadAll(mrResp.Body)
	// Very small manual multipart parse (enough for this test, works with binary bodies).
	sep := []byte("--" + boundary)
	chunks := bytes.Split(rawMR, sep)
	// chunks[0] is preamble (likely empty/CRLF). Then parts..., and the last chunk includes "--".
	var parts [][]byte
	for _, c := range chunks[1:] {
		c = bytes.TrimPrefix(c, []byte("\r\n"))
		if len(c) >= 2 && c[0] == '-' && c[1] == '-' {
			break // closing boundary
		}
		// headers end at \r\n\r\n
		i := bytes.Index(c, []byte("\r\n\r\n"))
		if i < 0 {
			t.Fatalf("multipart: missing header separator in %q", string(c))
		}
		body := c[i+4:]
		body = bytes.TrimSuffix(body, []byte("\r\n"))
		parts = append(parts, body)
	}
	if len(parts) != 2 {
		t.Fatalf("multipart parts=%d raw_len=%d raw_hex=%x", len(parts), len(rawMR), rawMR)
	}
	if !bytes.Equal(parts[0], payload[0:1]) {
		t.Fatalf("part1 mismatch len=%d head=%x want=%x", len(parts[0]), parts[0], payload[0:1])
	}
	if !bytes.Equal(parts[1], payload[2:4]) {
		t.Fatalf("part2 mismatch len=%d head=%x want=%x", len(parts[1]), parts[1], payload[2:4])
	}

	// If-Range/ETag: matching ETag should honor Range, mismatching should return 200 full.
	headReq, err := http.NewRequestWithContext(ctx, "GET", httpSrv.URL+"/v1/fs/content?path=/rg/a.bin", nil)
	if err != nil {
		t.Fatal(err)
	}
	headResp, err := http.DefaultClient.Do(headReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = headResp.Body.Close()
	etag := strings.TrimSpace(headResp.Header.Get("ETag"))
	if etag == "" {
		t.Fatal("missing ETag")
	}

	ifRangeOKReq, _ := http.NewRequestWithContext(ctx, "GET", httpSrv.URL+"/v1/fs/content?path=/rg/a.bin", nil)
	ifRangeOKReq.Header.Set("Range", "bytes=0-0")
	ifRangeOKReq.Header.Set("If-Range", etag)
	ifRangeOKResp, err := http.DefaultClient.Do(ifRangeOKReq)
	if err != nil {
		t.Fatal(err)
	}
	defer ifRangeOKResp.Body.Close()
	if ifRangeOKResp.StatusCode != http.StatusPartialContent {
		b, _ := io.ReadAll(ifRangeOKResp.Body)
		t.Fatalf("if-range match status=%d body=%q", ifRangeOKResp.StatusCode, string(b))
	}

	ifRangeBadReq, _ := http.NewRequestWithContext(ctx, "GET", httpSrv.URL+"/v1/fs/content?path=/rg/a.bin", nil)
	ifRangeBadReq.Header.Set("Range", "bytes=0-0")
	ifRangeBadReq.Header.Set("If-Range", `"does-not-match"`)
	ifRangeBadResp, err := http.DefaultClient.Do(ifRangeBadReq)
	if err != nil {
		t.Fatal(err)
	}
	defer ifRangeBadResp.Body.Close()
	if ifRangeBadResp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(ifRangeBadResp.Body)
		t.Fatalf("if-range mismatch status=%d body=%q", ifRangeBadResp.StatusCode, string(b))
	}

	// Conditional GET: If-None-Match should return 304.
	condReq, _ := http.NewRequestWithContext(ctx, "GET", httpSrv.URL+"/v1/fs/content?path=/rg/a.bin", nil)
	condReq.Header.Set("If-None-Match", etag)
	condResp, err := http.DefaultClient.Do(condReq)
	if err != nil {
		t.Fatal(err)
	}
	defer condResp.Body.Close()
	if condResp.StatusCode != http.StatusNotModified {
		b, _ := io.ReadAll(condResp.Body)
		t.Fatalf("if-none-match status=%d body=%q", condResp.StatusCode, string(b))
	}

	// HEAD should return validators and no body.
	headOnlyReq, _ := http.NewRequestWithContext(ctx, "HEAD", httpSrv.URL+"/v1/fs/content?path=/rg/a.bin", nil)
	headOnlyResp, err := http.DefaultClient.Do(headOnlyReq)
	if err != nil {
		t.Fatal(err)
	}
	defer headOnlyResp.Body.Close()
	if headOnlyResp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(headOnlyResp.Body)
		t.Fatalf("head status=%d body=%q", headOnlyResp.StatusCode, string(b))
	}
	if strings.TrimSpace(headOnlyResp.Header.Get("ETag")) == "" {
		t.Fatal("head missing ETag")
	}
	hb, _ := io.ReadAll(headOnlyResp.Body)
	if len(hb) != 0 {
		t.Fatalf("head body len=%d", len(hb))
	}

	// Snapshots smoke.
	snapB := mustOK(doJSON("POST", "/v1/snapshots", map[string]string{"label": "e2e"}))
	var snapResp struct {
		SnapshotID string `json:"snapshot_id"`
	}
	if err := json.Unmarshal(snapB, &snapResp); err != nil {
		t.Fatalf("snapshot json: %v", err)
	}
	if snapResp.SnapshotID == "" {
		t.Fatal("missing snapshot_id")
	}
	_ = mustOK(doJSON("GET", "/v1/snapshots", nil))
	_ = mustOK(doJSON("GET", "/v1/snapshots/"+snapResp.SnapshotID, nil))
	mustNoContent(doJSON("DELETE", "/v1/snapshots/"+snapResp.SnapshotID, nil))
}

func TestE2E_RESTGateway_MaxBody_413(t *testing.T) {
	const chunkSize = 256 * 1024
	_, cl := e2e.StartMaster(t, chunkSize, 1)
	dir := t.TempDir()
	cl.AddChunkServer(t, "chunk-a", filepath.Join(dir, "c0"))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	gwCli, err := client.NewGateway(cl.MasterAddr, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	defer gwCli.Close()

	srv := &restgateway.Server{Client: gwCli, MaxBody: 1024}
	mux := http.NewServeMux()
	srv.Register(mux)
	httpSrv := httptest.NewServer(mux)
	defer httpSrv.Close()

	reqJSON := func(method, path string, body any) {
		t.Helper()
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("json: %v", err)
		}
		req, err := http.NewRequestWithContext(ctx, method, httpSrv.URL+path, bytes.NewReader(b))
		if err != nil {
			t.Fatalf("req: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("do: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			rb, _ := io.ReadAll(resp.Body)
			t.Fatalf("status=%d body=%q", resp.StatusCode, string(rb))
		}
	}

	reqJSON("POST", "/v1/fs/mkdir", map[string]string{"path": "/m"})
	reqJSON("POST", "/v1/fs/file", map[string]string{"path": "/m/x"})

	tooBig := bytes.Repeat([]byte("a"), 2048)
	req, err := http.NewRequestWithContext(ctx, "PUT", httpSrv.URL+"/v1/fs/content?path=/m/x", bytes.NewReader(tooBig))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Length", strconv.Itoa(len(tooBig)))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		rb, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%q", resp.StatusCode, string(rb))
	}
}

func TestE2E_RESTGateway_CORS_Preflight(t *testing.T) {
	t.Setenv("GODFS_REST_CORS_ALLOW_ORIGINS", "http://example.com")

	const chunkSize = 256 * 1024
	_, cl := e2e.StartMaster(t, chunkSize, 1)
	dir := t.TempDir()
	cl.AddChunkServer(t, "chunk-a", filepath.Join(dir, "c0"))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	gwCli, err := client.NewGateway(cl.MasterAddr, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	defer gwCli.Close()

	srv := &restgateway.Server{Client: gwCli, MaxBody: 10 << 20}
	mux := http.NewServeMux()
	srv.Register(mux)
	handler := restgateway.WithCORS(mux)
	httpSrv := httptest.NewServer(handler)
	defer httpSrv.Close()

	req, err := http.NewRequestWithContext(ctx, "OPTIONS", httpSrv.URL+"/v1/fs/content?path=/x", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Origin", "http://example.com")
	req.Header.Set("Access-Control-Request-Method", "GET")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%q", resp.StatusCode, string(b))
	}
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "http://example.com" {
		t.Fatalf("allow-origin=%q", got)
	}
	if got := resp.Header.Get("Access-Control-Allow-Methods"); got == "" {
		t.Fatal("missing allow-methods")
	}
	if got := resp.Header.Get("Access-Control-Allow-Headers"); got == "" {
		t.Fatal("missing allow-headers")
	}
}

func TestE2E_RESTGateway_RateLimit_429(t *testing.T) {
	// Allow only 1 request immediately (burst=1) and no refill (rps very small).
	t.Setenv("GODFS_REST_RATE_LIMIT_RPS", "0.0001")
	t.Setenv("GODFS_REST_RATE_LIMIT_BURST", "1")

	const chunkSize = 256 * 1024
	_, cl := e2e.StartMaster(t, chunkSize, 1)
	dir := t.TempDir()
	cl.AddChunkServer(t, "chunk-a", filepath.Join(dir, "c0"))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	gwCli, err := client.NewGateway(cl.MasterAddr, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	defer gwCli.Close()

	srv := &restgateway.Server{Client: gwCli, MaxBody: 10 << 20}
	mux := http.NewServeMux()
	srv.Register(mux)
	handler := restgateway.WithRateLimit(mux)
	httpSrv := httptest.NewServer(handler)
	defer httpSrv.Close()

	// Two quick requests with the same Authorization should hit the same bucket.
	auth := "Bearer test-token"
	do := func() *http.Response {
		req, err := http.NewRequestWithContext(ctx, "GET", httpSrv.URL+"/v1/health", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", auth)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	r1 := do()
	_ = r1.Body.Close()
	if r1.StatusCode != http.StatusOK {
		t.Fatalf("first status=%d", r1.StatusCode)
	}

	r2 := do()
	defer r2.Body.Close()
	if r2.StatusCode != http.StatusTooManyRequests {
		b, _ := io.ReadAll(r2.Body)
		t.Fatalf("second status=%d body=%q", r2.StatusCode, string(b))
	}
}

