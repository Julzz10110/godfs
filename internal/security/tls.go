package security

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"strings"

	"google.golang.org/grpc/credentials"
)

// TLSConfig holds paths loaded from environment for server and client TLS.
type TLSConfig struct {
	Enabled        bool
	CertFile       string
	KeyFile        string
	CAFile         string
	ClientCertFile string
	ClientKeyFile  string
}

// LoadTLSConfigFromEnv reads GODFS_TLS_* variables.
// GODFS_TLS_ENABLED=1 (or true) enables TLS; cert/key required for servers, CA for verifying peers.
func LoadTLSConfigFromEnv() TLSConfig {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("GODFS_TLS_ENABLED")))
	enabled := v == "1" || v == "true" || v == "yes"
	return TLSConfig{
		Enabled:        enabled,
		CertFile:       firstNonEmpty(os.Getenv("GODFS_TLS_CERT_FILE"), os.Getenv("GODFS_TLS_SERVER_CERT")),
		KeyFile:        firstNonEmpty(os.Getenv("GODFS_TLS_KEY_FILE"), os.Getenv("GODFS_TLS_SERVER_KEY")),
		CAFile:         firstNonEmpty(os.Getenv("GODFS_TLS_CA_FILE"), os.Getenv("GODFS_TLS_CA")),
		ClientCertFile: firstNonEmpty(os.Getenv("GODFS_TLS_CLIENT_CERT_FILE"), os.Getenv("GODFS_TLS_CLIENT_CERT")),
		ClientKeyFile:  firstNonEmpty(os.Getenv("GODFS_TLS_CLIENT_KEY_FILE"), os.Getenv("GODFS_TLS_CLIENT_KEY")),
	}
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// ServerTransportCredentials returns TLS server credentials (TLS 1.3+).
// If CAFile is set, mTLS is required (RequireAndVerifyClientCert).
func ServerTransportCredentials(cfg TLSConfig) (credentials.TransportCredentials, error) {
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, err
	}
	tlsConf := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
	}
	if cfg.CAFile != "" {
		caPEM, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, errors.New("invalid CA PEM")
		}
		tlsConf.ClientCAs = pool
		tlsConf.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return credentials.NewTLS(tlsConf), nil
}

// ClientTransportCredentials returns TLS client credentials; optional mTLS if client cert+key set.
func ClientTransportCredentials(cfg TLSConfig) (credentials.TransportCredentials, error) {
	tlsConf := &tls.Config{MinVersion: tls.VersionTLS13}
	if cfg.CAFile != "" {
		caPEM, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, errors.New("invalid CA PEM")
		}
		tlsConf.RootCAs = pool
	}
	if cfg.ClientCertFile != "" && cfg.ClientKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.ClientCertFile, cfg.ClientKeyFile)
		if err != nil {
			return nil, err
		}
		tlsConf.Certificates = []tls.Certificate{cert}
	}
	return credentials.NewTLS(tlsConf), nil
}
