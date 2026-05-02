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

	httpReqsTotal          *prometheus.CounterVec
	httpReqDurations       *prometheus.HistogramVec
	httpReqInFlight        prometheus.Gauge
	httpResponseBytesTotal *prometheus.CounterVec
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
		httpResponseBytesTotal = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "godfs",
				Subsystem: "rest",
				Name:      "http_response_bytes_total",
				Help:      "Total bytes written in HTTP responses by the REST gateway.",
			},
			[]string{"method", "route"},
		)

		registerCounterVec := func(c *prometheus.CounterVec, assign **prometheus.CounterVec) {
			if err := prometheus.Register(c); err != nil {
				if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
					if v, ok := are.ExistingCollector.(*prometheus.CounterVec); ok {
						*assign = v
					}
				}
			}
		}
		registerHistogramVec := func(c *prometheus.HistogramVec) {
			if err := prometheus.Register(c); err != nil {
				if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
					if v, ok := are.ExistingCollector.(*prometheus.HistogramVec); ok {
						httpReqDurations = v
					}
				}
			}
		}
		registerGauge := func(c prometheus.Gauge) {
			if err := prometheus.Register(c); err != nil {
				if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
					if v, ok := are.ExistingCollector.(prometheus.Gauge); ok {
						httpReqInFlight = v
					}
				}
			}
		}

		registerCounterVec(httpReqsTotal, &httpReqsTotal)
		registerHistogramVec(httpReqDurations)
		registerGauge(httpReqInFlight)
		registerCounterVec(httpResponseBytesTotal, &httpResponseBytesTotal)
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

type countingResponseWriter struct {
	http.ResponseWriter
	n int64
}

func (w *countingResponseWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	w.n += int64(n)
	return n, err
}

func (w *countingResponseWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// WithHTTPMetrics instruments a handler with Prometheus metrics.
func WithHTTPMetrics(next http.Handler) http.Handler {
	ensureHTTPMetricsRegistered()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cw := &countingResponseWriter{ResponseWriter: w}
		sw := &statusWriter{ResponseWriter: cw, status: http.StatusOK}
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
			httpResponseBytesTotal.WithLabelValues(r.Method, route).Add(float64(cw.n))
		}()
		next.ServeHTTP(sw, r)
	})
}
