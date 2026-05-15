package restgateway

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func presignHMACSecret() []byte {
	return []byte(strings.TrimSpace(os.Getenv("GODFS_REST_PRESIGN_HMAC_SECRET")))
}

func presignPayload(method, path string, expUnix int64) string {
	return "v1|" + method + "|" + path + "|" + strconv.FormatInt(expUnix, 10)
}

// PresignSignature returns hex HMAC-SHA256 for content presign (GET or PUT).
func PresignSignature(secret []byte, method, path string, expUnix int64) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(presignPayload(method, path, expUnix)))
	return hex.EncodeToString(mac.Sum(nil))
}

// PresignGETSignature returns hex HMAC-SHA256 for GET content presign query validation.
func PresignGETSignature(secret []byte, path string, expUnix int64) string {
	return PresignSignature(secret, http.MethodGet, path, expUnix)
}

// PresignPUTSignature returns hex HMAC-SHA256 for PUT content presign query validation.
func PresignPUTSignature(secret []byte, path string, expUnix int64) string {
	return PresignSignature(secret, http.MethodPut, path, expUnix)
}

func presignedValid(r *http.Request, path string, method string) bool {
	sec := presignHMACSecret()
	if len(sec) < 16 {
		return false
	}
	expStr := r.URL.Query().Get("godfs_exp")
	sig := strings.TrimSpace(r.URL.Query().Get("godfs_sig"))
	if expStr == "" || sig == "" {
		return false
	}
	expUnix, err := strconv.ParseInt(expStr, 10, 64)
	if err != nil || expUnix <= 0 {
		return false
	}
	if time.Now().Unix() > expUnix {
		return false
	}
	want := PresignSignature(sec, method, path, expUnix)
	return hmac.Equal([]byte(strings.ToLower(sig)), []byte(strings.ToLower(want)))
}

func presignedGETValid(r *http.Request, path string) bool {
	return presignedValid(r, path, http.MethodGet)
}

func presignedPUTValid(r *http.Request, path string) bool {
	return presignedValid(r, path, http.MethodPut)
}

// presignEnforced is true when the gateway requires Bearer or valid presign query on /v1/fs/content.
func presignEnforced() bool {
	return len(presignHMACSecret()) >= 16
}

// contentAuthOK returns true when Authorization is set or presign query is valid for the HTTP method.
func contentAuthOK(r *http.Request, path string) bool {
	if strings.TrimSpace(r.Header.Get("Authorization")) != "" {
		return true
	}
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		return presignedGETValid(r, path)
	case http.MethodPut:
		return presignedPUTValid(r, path)
	default:
		return false
	}
}

// requireContentAuth rejects anonymous content access when presign secret is configured.
func requireContentAuth(w http.ResponseWriter, r *http.Request, path string) bool {
	if !presignEnforced() {
		return true
	}
	if contentAuthOK(r, path) {
		return true
	}
	writeJSON(w, http.StatusUnauthorized, errJSON{Error: "authorization or valid presign required", Code: "unauthorized"})
	return false
}
