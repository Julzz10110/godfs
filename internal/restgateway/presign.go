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

// PresignGETSignature returns hex HMAC-SHA256 for GET content presign query validation.
func PresignGETSignature(secret []byte, path string, expUnix int64) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(presignPayload(http.MethodGet, path, expUnix)))
	return hex.EncodeToString(mac.Sum(nil))
}

func presignedGETValid(r *http.Request, path string) bool {
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
	want := PresignGETSignature(sec, path, expUnix)
	return hmac.Equal([]byte(strings.ToLower(sig)), []byte(strings.ToLower(want)))
}
