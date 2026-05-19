//go:build integration

package integration_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// errAfterReader returns data then a non-EOF error (simulates client disconnect mid-upload).
type errAfterReader struct {
	data []byte
	off  int
}

func (r *errAfterReader) Read(p []byte) (int, error) {
	if r.off >= len(r.data) {
		return 0, io.ErrUnexpectedEOF
	}
	n := copy(p, r.data[r.off:])
	r.off += n
	if r.off >= len(r.data) {
		return n, io.ErrUnexpectedEOF
	}
	return n, nil
}

// aborted PUT must not leave a fully readable object at the declared size.
func TestREST_PartialPutDoesNotCommitFullObject(t *testing.T) {
	base := restBaseURL()
	slug := strings.ReplaceAll(t.Name(), "/", "_")
	path := "/itest_partial_" + slug + "/blob.bin"
	client := restClient(t)
	auth := authHeader()

	mkdirURL := base + "/v1/fs/mkdir"
	body, _ := json.Marshal(map[string]string{"path": path[:strings.LastIndex(path, "/")]})
	req, err := http.NewRequest(http.MethodPost, mkdirURL, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	requireSuccessMutate(t, resp, "mkdir")

	createURL := base + "/v1/fs/file"
	body, _ = json.Marshal(map[string]string{"path": path})
	req, err = http.NewRequest(http.MethodPost, createURL, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	requireSuccessMutate(t, resp, "create")

	want := bytes.Repeat([]byte("x"), 64*1024)
	putURL := base + "/v1/fs/content?" + url.Values{"path": {path}}.Encode()
	req, err = http.NewRequest(http.MethodPut, putURL, &errAfterReader{data: want})
	if err != nil {
		t.Fatal(err)
	}
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	resp, err = client.Do(req)
	if err == nil {
		resp.Body.Close()
	}
	if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		t.Fatal("expected PUT failure on disconnect, got success")
	}

	getURL := base + "/v1/fs/content?" + url.Values{"path": {path}}.Encode()
	req, err = http.NewRequest(http.MethodGet, getURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNotFound, http.StatusBadRequest:
		return
	case http.StatusOK:
		if len(got) == len(want) && bytes.Equal(got, want) {
			t.Fatal("full object readable after aborted PUT")
		}
		if len(got) > 0 && len(got) < len(want) {
			t.Logf("partial body len=%d (acceptable; chunk may have committed before abort)", len(got))
			return
		}
		if len(got) == 0 {
			return
		}
		t.Fatalf("unexpected GET body len=%d status=%d", len(got), resp.StatusCode)
	default:
		t.Fatalf("GET status %d body len=%d", resp.StatusCode, len(got))
	}
}
