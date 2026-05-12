package security

import (
	"os"
	"strings"
)

// LoadRESTListenTLSFromEnv configures the REST gateway inbound HTTPS listener only.
// Outbound gRPC to the master still uses [LoadTLSConfigFromEnv] and [ClientTransport].
//
// Set GODFS_REST_HTTPS_ENABLED=1 (or true) and provide certificate paths:
//   - GODFS_REST_TLS_CERT_FILE / GODFS_REST_TLS_KEY_FILE, or
//   - fallback to GODFS_TLS_CERT_FILE / GODFS_TLS_KEY_FILE when REST paths are empty.
//
// Optional mTLS for REST clients: GODFS_REST_TLS_CA_FILE (PEM of client CAs).
// Hot reload uses the same GODFS_TLS_RELOAD and GODFS_TLS_RELOAD_INTERVAL as gRPC.
func LoadRESTListenTLSFromEnv() TLSConfig {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("GODFS_REST_HTTPS_ENABLED")))
	enabled := v == "1" || v == "true" || v == "yes"
	if !enabled {
		return TLSConfig{}
	}
	cert := firstNonEmpty(os.Getenv("GODFS_REST_TLS_CERT_FILE"), firstNonEmpty(os.Getenv("GODFS_TLS_CERT_FILE"), os.Getenv("GODFS_TLS_SERVER_CERT")))
	key := firstNonEmpty(os.Getenv("GODFS_REST_TLS_KEY_FILE"), firstNonEmpty(os.Getenv("GODFS_TLS_KEY_FILE"), os.Getenv("GODFS_TLS_SERVER_KEY")))
	ca := strings.TrimSpace(os.Getenv("GODFS_REST_TLS_CA_FILE"))
	return TLSConfig{
		Enabled:  true,
		CertFile: cert,
		KeyFile:  key,
		CAFile:   ca,
	}
}
