package security

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
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
	// ExtraCAFile is optional; PEM certs are appended to the trust pool (dual CA / bridge rotation).
	ExtraCAFile    string
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
		ExtraCAFile:    strings.TrimSpace(os.Getenv("GODFS_TLS_EXTRA_CA_FILE")),
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

// buildServerTLSConfig returns a tls.Config for gRPC or net/http listeners (TLS 1.3+).
// If CAFile is set, mTLS is required (RequireAndVerifyClientCert).
func buildServerTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	if tlsReloadEnabled() {
		r, err := newCertReloader(cfg)
		if err != nil {
			return nil, err
		}
		return &tls.Config{
			MinVersion:         tls.VersionTLS13,
			GetCertificate:     r.getServerCert,
			GetConfigForClient: r.serverConfigForClient,
		}, nil
	}

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
		if p, err := appendCertsFromFile(pool, cfg.ExtraCAFile); err != nil {
			return nil, err
		} else {
			pool = p
		}
		tlsConf.ClientCAs = pool
		tlsConf.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return tlsConf, nil
}

// ServerTransportCredentials returns TLS server credentials (TLS 1.3+).
// If CAFile is set, mTLS is required (RequireAndVerifyClientCert).
func ServerTransportCredentials(cfg TLSConfig) (credentials.TransportCredentials, error) {
	tlsConf, err := buildServerTLSConfig(cfg)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(tlsConf), nil
}

// HTTPServerTLSConfig returns a *tls.Config for net/http when cfg.Enabled.
// When cfg.Enabled is false, returns (nil, nil) for plain HTTP.
// When enabled, CertFile and KeyFile must be non-empty.
func HTTPServerTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	if cfg.CertFile == "" || cfg.KeyFile == "" {
		return nil, fmt.Errorf("HTTPS requires certificate and key file paths")
	}
	return buildServerTLSConfig(cfg)
}

// ClientTransportCredentials returns TLS client credentials; optional mTLS if client cert+key set.
func ClientTransportCredentials(cfg TLSConfig) (credentials.TransportCredentials, error) {
	if tlsReloadEnabled() {
		r, err := newCertReloader(cfg)
		if err != nil {
			return nil, err
		}
		tlsConf := &tls.Config{
			MinVersion:           tls.VersionTLS13,
			GetClientCertificate: r.getClientCert,
		}
		// RootCAs rotation is not handled here; baseline is server-side CA rotation.
		if cfg.CAFile != "" {
			caPEM, err := os.ReadFile(cfg.CAFile)
			if err != nil {
				return nil, err
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(caPEM) {
				return nil, errors.New("invalid CA PEM")
			}
			if p, err := appendCertsFromFile(pool, cfg.ExtraCAFile); err != nil {
				return nil, err
			} else {
				pool = p
			}
			tlsConf.RootCAs = pool
		} else if cfg.ExtraCAFile != "" {
			pool, err := appendCertsFromFile(nil, cfg.ExtraCAFile)
			if err != nil {
				return nil, err
			}
			tlsConf.RootCAs = pool
		}
		return credentials.NewTLS(tlsConf), nil
	}

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
		if p, err := appendCertsFromFile(pool, cfg.ExtraCAFile); err != nil {
			return nil, err
		} else {
			pool = p
		}
		tlsConf.RootCAs = pool
	} else if cfg.ExtraCAFile != "" {
		pool, err := appendCertsFromFile(nil, cfg.ExtraCAFile)
		if err != nil {
			return nil, err
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

// appendCertsFromFile appends PEM certificates from path to pool (creates a pool if nil).
func appendCertsFromFile(pool *x509.CertPool, path string) (*x509.CertPool, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return pool, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if pool == nil {
		pool = x509.NewCertPool()
	}
	if !pool.AppendCertsFromPEM(b) {
		return nil, fmt.Errorf("invalid CA PEM in %s", path)
	}
	return pool, nil
}
