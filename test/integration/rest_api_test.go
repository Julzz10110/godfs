//go:build integration

package integration_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

func restBaseURL() string {
	if u := strings.TrimSpace(os.Getenv("GODFS_REST_BASE_URL")); u != "" {
		return strings.TrimRight(u, "/")
	}
	return "http://127.0.0.1:8080"
}

func restClient(t *testing.T) *http.Client {
	t.Helper()
	return &http.Client{Timeout: 30 * time.Second}
}

func authHeader() string {
	if k := strings.TrimSpace(os.Getenv("GODFS_TEST_API_KEY")); k != "" {
		return "Bearer " + k
	}
	return ""
}

func TestREST_Health(t *testing.T) {
	c := restClient(t)
	req, _ := http.NewRequest(http.MethodGet, restBaseURL()+"/v1/health", nil)
	if h := authHeader(); h != "" {
		req.Header.Set("Authorization", h)
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health status %d", resp.StatusCode)
	}
}

func TestREST_MKdirPutGet(t *testing.T) {
	base := restBaseURL()
	path := "/itest/" + t.Name()
	client := restClient(t)
	auth := authHeader()

	doJSON := func(method, url string, body any) *http.Response {
		t.Helper()
		var rdr io.Reader
		if body != nil {
			b, err := json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
			rdr = bytes.NewReader(b)
		}
		req, err := http.NewRequest(method, url, rdr)
		if err != nil {
			t.Fatal(err)
		}
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	resp := doJSON(http.MethodPost, base+"/v1/fs/mkdir", map[string]string{"path": path})
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("mkdir %s: %d", path, resp.StatusCode)
	}

	filePath := path + "/blob.bin"
	resp = doJSON(http.MethodPost, base+"/v1/fs/file", map[string]string{"path": filePath})
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create file: %d", resp.StatusCode)
	}

	putBody := []byte("integration-" + t.Name())
	putURL := base + "/v1/fs/content?" + url.Values{"path": {filePath}}.Encode()
	req, err := http.NewRequest(http.MethodPut, putURL, bytes.NewReader(putBody))
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
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("put: %d %s", resp.StatusCode, string(b))
	}

	getURL := base + "/v1/fs/content?" + url.Values{"path": {filePath}}.Encode()
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
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get: %d", resp.StatusCode)
	}
	if string(got) != string(putBody) {
		t.Fatalf("body mismatch: got %q want %q", got, putBody)
	}
}
