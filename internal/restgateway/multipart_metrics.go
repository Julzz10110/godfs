package restgateway

import (
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	multipartMetricsOnce sync.Once

	multipartUploadsActive    prometheus.Gauge
	multipartPartsStagedBytes prometheus.Gauge
)

func ensureMultipartMetricsRegistered() {
	multipartMetricsOnce.Do(func() {
		multipartUploadsActive = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "godfs",
			Subsystem: "rest",
			Name:      "multipart_uploads_active",
			Help:      "Number of in-progress REST multipart uploads (staging directories with manifest).",
		})
		multipartPartsStagedBytes = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "godfs",
			Subsystem: "rest",
			Name:      "multipart_parts_staged_bytes",
			Help:      "Total bytes of uploaded multipart parts on disk in the REST gateway staging directory.",
		})
		registerGauge := func(c prometheus.Gauge, assign *prometheus.Gauge) {
			*assign = c
			if err := prometheus.Register(c); err != nil {
				if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
					if v, ok := are.ExistingCollector.(prometheus.Gauge); ok {
						*assign = v
					}
				}
			}
		}
		registerGauge(multipartUploadsActive, &multipartUploadsActive)
		registerGauge(multipartPartsStagedBytes, &multipartPartsStagedBytes)
	})
}

func incMultipartUploadsActive() {
	ensureMultipartMetricsRegistered()
	if multipartUploadsActive != nil {
		multipartUploadsActive.Inc()
	}
}

func decMultipartUploadsActive() {
	ensureMultipartMetricsRegistered()
	if multipartUploadsActive != nil {
		multipartUploadsActive.Dec()
	}
}

func addMultipartStagedBytes(delta int64) {
	ensureMultipartMetricsRegistered()
	if multipartPartsStagedBytes == nil || delta == 0 {
		return
	}
	if delta > 0 {
		multipartPartsStagedBytes.Add(float64(delta))
	} else {
		multipartPartsStagedBytes.Sub(float64(-delta))
	}
}

func setMultipartMetricsFromDisk(active int, stagedBytes int64) {
	ensureMultipartMetricsRegistered()
	if multipartUploadsActive != nil {
		multipartUploadsActive.Set(float64(active))
	}
	if multipartPartsStagedBytes != nil {
		multipartPartsStagedBytes.Set(float64(stagedBytes))
	}
}

func stagedBytesInUploadDir(dir string) int64 {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	var total int64
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), "part-") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		total += info.Size()
	}
	return total
}

func reconcileMultipartMetricsFromDisk(baseDir string) {
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			setMultipartMetricsFromDisk(0, 0)
		}
		return
	}
	var active int
	var staged int64
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(baseDir, e.Name())
		if _, err := os.Stat(filepath.Join(dir, multipartManifestFile)); err != nil {
			continue
		}
		active++
		staged += stagedBytesInUploadDir(dir)
	}
	setMultipartMetricsFromDisk(active, staged)
}
