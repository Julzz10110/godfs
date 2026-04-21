package restgateway

import (
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	metricsOnce sync.Once

	httpReqsTotal    *prometheus.CounterVec
	httpReqDurations *prometheus.HistogramVec
	httpReqInFlight  prometheus.Gauge
)

func ensureHTTPMetricsRegistered() {
	metricsOnce.Do(func() {
		httpReqsTotal = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "godfs",
				Subsystem: "rest",
				Name:      "http_requests_total",
				Help:      "Total number of HTTP requests handled by the REST gateway.",
			},
			[]string{"method", "route", "status"},
		)
		httpReqDurations = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "godfs",
				Subsystem: "rest",
				Name:      "http_request_duration_seconds",
				Help:      "HTTP request duration in seconds for the REST gateway.",
				Buckets:   prometheus.DefBuckets,
			},
			[]string{"method", "route"},
		)
		httpReqInFlight = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "godfs",
			Subsystem: "rest",
			Name:      "http_requests_in_flight",
			Help:      "Number of REST gateway HTTP requests currently being served.",
		})

		register := func(c prometheus.Collector) {
			if err := prometheus.Register(c); err != nil {
				if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
					switch v := are.ExistingCollector.(type) {
					case *prometheus.CounterVec:
						httpReqsTotal = v
					case *prometheus.HistogramVec:
						httpReqDurations = v
					case prometheus.Gauge:
						httpReqInFlight = v
					}
				}
			}
		}

		register(httpReqsTotal)
		register(httpReqDurations)
		register(httpReqInFlight)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// WithHTTPMetrics instruments a handler with Prometheus metrics.
func WithHTTPMetrics(next http.Handler) http.Handler {
	ensureHTTPMetricsRegistered()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		route := r.Pattern
		if route == "" {
			route = "unknown"
		}
		httpReqInFlight.Inc()
		start := time.Now()
		defer func() {
			httpReqInFlight.Dec()
			httpReqDurations.WithLabelValues(r.Method, route).Observe(time.Since(start).Seconds())
			httpReqsTotal.WithLabelValues(r.Method, route, strconv.Itoa(sw.status)).Inc()
		}()
		next.ServeHTTP(sw, r)
	})
}

