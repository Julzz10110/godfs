package restgateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strings"
)

type ctxKeyRequestID struct{}

// WithRequestID ensures X-Request-ID on the response and in context for error correlation.
func WithRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSpace(r.Header.Get("X-Request-ID"))
		if id == "" {
			id = newRequestID()
		}
		ctx := context.WithValue(r.Context(), ctxKeyRequestID{}, id)
		r = r.WithContext(ctx)
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r)
	})
}

func newRequestID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "req-unknown"
	}
	return hex.EncodeToString(b[:])
}

// RequestIDFromContext returns the X-Request-ID value or empty.
func RequestIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(ctxKeyRequestID{}).(string)
	return v
}
