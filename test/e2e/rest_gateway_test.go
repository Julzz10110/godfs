package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
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

