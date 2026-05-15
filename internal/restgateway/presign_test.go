package restgateway

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

func TestPresignPUT_ValidAndExpired(t *testing.T) {
	secret := []byte("0123456789abcdef")
	path := "/data/file.bin"
	exp := time.Now().Add(5 * time.Minute).Unix()
	sig := PresignPUTSignature(secret, path, exp)

	r := httptest.NewRequest(http.MethodPut, "/v1/fs/content?path="+path+"&godfs_exp="+strconv.FormatInt(exp, 10)+"&godfs_sig="+sig, nil)
	t.Setenv("GODFS_REST_PRESIGN_HMAC_SECRET", string(secret))
	if !presignedPUTValid(r, path) {
		t.Fatal("expected valid presigned PUT")
	}

	past := time.Now().Add(-time.Minute).Unix()
	r2 := httptest.NewRequest(http.MethodPut, "/v1/fs/content?path="+path+"&godfs_exp="+strconv.FormatInt(past, 10)+"&godfs_sig="+sig, nil)
	if presignedPUTValid(r2, path) {
		t.Fatal("expected expired presign to fail")
	}
}

func TestPresignGET_StillValid(t *testing.T) {
	secret := []byte("0123456789abcdef")
	path := "/a/b"
	exp := time.Now().Add(time.Minute).Unix()
	sig := PresignGETSignature(secret, path, exp)
	t.Setenv("GODFS_REST_PRESIGN_HMAC_SECRET", string(secret))

	r := httptest.NewRequest(http.MethodGet, "/v1/fs/content?path="+path+"&godfs_exp="+strconv.FormatInt(exp, 10)+"&godfs_sig="+sig, nil)
	if !presignedGETValid(r, path) {
		t.Fatal("expected valid GET presign")
	}
}

func TestRequireContentAuth_PresignEnforced(t *testing.T) {
	t.Setenv("GODFS_REST_PRESIGN_HMAC_SECRET", "0123456789abcdef")
	path := "/x"

	r := httptest.NewRequest(http.MethodPut, "/v1/fs/content?path="+path, nil)
	rr := httptest.NewRecorder()
	if requireContentAuth(rr, r, path) {
		t.Fatal("expected reject without auth")
	}
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d", rr.Code)
	}
}

func TestRequireContentAuth_BearerBypassesPresign(t *testing.T) {
	t.Setenv("GODFS_REST_PRESIGN_HMAC_SECRET", "0123456789abcdef")
	path := "/x"
	r := httptest.NewRequest(http.MethodPut, "/v1/fs/content?path="+path, nil)
	r.Header.Set("Authorization", "Bearer user-key")
	rr := httptest.NewRecorder()
	if !requireContentAuth(rr, r, path) {
		t.Fatal("expected allow with bearer")
	}
}
