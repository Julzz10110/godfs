package observability

import (
	"log"
	"net/http"
	"strings"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// StartMetricsHTTPServer serves Prometheus scrape endpoint at /metrics when addr is non-empty (e.g. ":9091").
func StartMetricsHTTPServer(addr string) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return
	}
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		log.Printf("prometheus metrics listening on http://%s/metrics", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Fatalf("metrics http: %v", err)
		}
	}()
}
