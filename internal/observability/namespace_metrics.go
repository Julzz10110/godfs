package observability

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	nsOnce sync.Once

	nsFilesGauge   prometheus.Gauge
	nsDirsGauge    prometheus.Gauge
	nsChunksGauge  prometheus.Gauge
	nsLogicalBytes prometheus.Gauge
)

// InitNamespaceMetrics registers namespace scale gauges (SRE / capacity planning).
func InitNamespaceMetrics() {
	nsOnce.Do(func() {
		nsFilesGauge = mustRegisterGauge(prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "godfs_metadata_files_total",
			Help: "Number of file paths in the metadata namespace (best-effort, leader view for Raft).",
		}))
		nsDirsGauge = mustRegisterGauge(prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "godfs_metadata_dirs_total",
			Help: "Number of directory paths in the metadata namespace.",
		}))
		nsChunksGauge = mustRegisterGauge(prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "godfs_metadata_chunks_total",
			Help: "Number of chunk objects tracked in metadata.",
		}))
		nsLogicalBytes = mustRegisterGauge(prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "godfs_metadata_logical_bytes",
			Help: "Sum of logical file sizes in metadata (not physical replica bytes).",
		}))
	})
}

// SetNamespaceSnapshot updates namespace scale gauges (no-op if InitNamespaceMetrics was not called).
func SetNamespaceSnapshot(files, dirs, chunks int, logicalBytes int64) {
	if nsFilesGauge == nil {
		return
	}
	nsFilesGauge.Set(float64(files))
	nsDirsGauge.Set(float64(dirs))
	nsChunksGauge.Set(float64(chunks))
	nsLogicalBytes.Set(float64(logicalBytes))
}
