package security

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"strings"
	"sync"
	"time"
)

func tlsReloadEnabled() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("GODFS_TLS_RELOAD")))
	return v == "1" || v == "true" || v == "yes"
}

func tlsReloadInterval() time.Duration {
	v := strings.TrimSpace(os.Getenv("GODFS_TLS_RELOAD_INTERVAL"))
	if v == "" {
		return 5 * time.Second
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 5 * time.Second
	}
	// avoid overly tight loops
	if d < 500*time.Millisecond {
		return 500 * time.Millisecond
	}
	return d
}

// certReloader reloads cert/key/ca files periodically and serves the latest material
// through tls.Config callbacks.
type certReloader struct {
	cfg TLSConfig

	mu sync.RWMutex
	// updated under mu
	serverCert *tls.Certificate
	clientCert *tls.Certificate
	clientCAs  *x509.CertPool
}

func newCertReloader(cfg TLSConfig) (*certReloader, error) {
	r := &certReloader{cfg: cfg}
	if err := r.reload(); err != nil {
		return nil, err
	}
	go r.loop()
	return r, nil
}

func (r *certReloader) loop() {
	t := time.NewTicker(tlsReloadInterval())
	defer t.Stop()
	for range t.C {
		_ = r.reload()
	}
}

func (r *certReloader) reload() error {
	var (
		srvCert *tls.Certificate
		cliCert *tls.Certificate
		cas     *x509.CertPool
	)

	if r.cfg.CertFile != "" && r.cfg.KeyFile != "" {
		c, err := tls.LoadX509KeyPair(r.cfg.CertFile, r.cfg.KeyFile)
		if err != nil {
			return err
		}
		srvCert = &c
	}
	if r.cfg.ClientCertFile != "" && r.cfg.ClientKeyFile != "" {
		c, err := tls.LoadX509KeyPair(r.cfg.ClientCertFile, r.cfg.ClientKeyFile)
		if err != nil {
			return err
		}
		cliCert = &c
	}
	if r.cfg.CAFile != "" {
		caPEM, err := os.ReadFile(r.cfg.CAFile)
		if err != nil {
			return err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return errors.New("invalid CA PEM")
		}
		cas = pool
	}

	r.mu.Lock()
	r.serverCert = srvCert
	r.clientCert = cliCert
	r.clientCAs = cas
	r.mu.Unlock()
	return nil
}

func (r *certReloader) getServerCert(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.serverCert == nil {
		return nil, errors.New("server cert not configured")
	}
	return r.serverCert, nil
}

func (r *certReloader) getClientCert(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.clientCert == nil {
		// mTLS optional for clients; return empty cert without error.
		return &tls.Certificate{}, nil
	}
	return r.clientCert, nil
}

func (r *certReloader) serverConfigForClient(chi *tls.ClientHelloInfo) (*tls.Config, error) {
	_ = chi
	r.mu.RLock()
	defer r.mu.RUnlock()
	// Copy minimal config each handshake so ClientCAs can rotate.
	cfg := &tls.Config{
		MinVersion:     tls.VersionTLS13,
		GetCertificate: r.getServerCert,
	}
	if r.clientCAs != nil {
		cfg.ClientCAs = r.clientCAs
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}
