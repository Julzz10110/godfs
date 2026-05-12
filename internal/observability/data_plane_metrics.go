package observability

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

type DataPlaneActionType string

const (
	ActionAddReplica  DataPlaneActionType = "add_replica"
	ActionRepairStale DataPlaneActionType = "repair_stale"
)

type InFlightKind string

const (
	InFlightPull     InFlightKind = "pull"
	InFlightDelete   InFlightKind = "delete"
	InFlightChecksum InFlightKind = "checksum"
)

var (
	dpOnce sync.Once

	dpUnderReplicatedChunks prometheus.Gauge
	dpStaleReplicas         prometheus.Gauge
	dpPendingDeletes        prometheus.Gauge
	dpUnrepairableChunks    prometheus.Gauge

	maintInFlight *prometheus.GaugeVec

	rebalanceActionsTotal *prometheus.CounterVec
	rebalanceErrorsTotal  *prometheus.CounterVec
	deleteActionsTotal    prometheus.Counter
	deleteErrorsTotal     *prometheus.CounterVec

	maintChecksumRPCTotal        *prometheus.CounterVec
	maintReplicaMetaCompareTotal *prometheus.CounterVec
)

// InitDataPlaneMetrics registers Production-2 data plane metrics.
// Safe to call multiple times.
func InitDataPlaneMetrics() {
	dpOnce.Do(func() {
		dpUnderReplicatedChunks = mustRegisterGauge(prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "godfs_data_under_replicated_chunks",
			Help: "Number of chunks that currently have fewer live replicas than the target replication factor (best-effort).",
		}))
		dpStaleReplicas = mustRegisterGauge(prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "godfs_data_stale_replicas",
			Help: "Number of stale replicas detected by checksum verification (best-effort).",
		}))
		dpPendingDeletes = mustRegisterGauge(prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "godfs_data_pending_deletes",
			Help: "Number of pending DeleteChunk actions across all peers (best-effort).",
		}))
		dpUnrepairableChunks = mustRegisterGauge(prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "godfs_data_unrepairable_chunks",
			Help: "Number of chunks marked unrepairable by the rebalancer (no good replica found).",
		}))

		maintInFlight = mustRegisterGaugeVec(prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "godfs_maint_inflight",
			Help: "Number of in-flight background maintenance operations.",
		}, []string{"kind"}))

		rebalanceActionsTotal = mustRegisterCounterVec(prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "godfs_maint_rebalance_actions_total",
			Help: "Total number of rebalancer actions executed.",
		}, []string{"type"}))
		rebalanceErrorsTotal = mustRegisterCounterVec(prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "godfs_maint_rebalance_errors_total",
			Help: "Total number of rebalancer action errors.",
		}, []string{"type", "reason"}))

		deleteActionsTotal = mustRegisterCounter(prometheus.NewCounter(prometheus.CounterOpts{
			Name: "godfs_maint_delete_actions_total",
			Help: "Total number of best-effort DeleteChunk actions executed by GC.",
		}))
		deleteErrorsTotal = mustRegisterCounterVec(prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "godfs_maint_delete_errors_total",
			Help: "Total number of best-effort DeleteChunk action errors.",
		}, []string{"reason"}))

		maintChecksumRPCTotal = mustRegisterCounterVec(prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "godfs_maint_checksum_rpc_total",
			Help: "ChecksumChunk RPCs issued by master background maintenance (verifier wrapper).",
		}, []string{"result"}))
		maintReplicaMetaCompareTotal = mustRegisterCounterVec(prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "godfs_maint_replica_meta_compare_total",
			Help: "Replica SHA-256 compared to committed metadata checksum during scans / stale counting.",
		}, []string{"result"}))
	})
}

type DataPlaneStats struct {
	UnderReplicatedChunks int
	StaleReplicas         int
	PendingDeletes        int
	UnrepairableChunks    int
}

func SetDataPlaneStats(st DataPlaneStats) {
	if dpUnderReplicatedChunks != nil {
		dpUnderReplicatedChunks.Set(float64(st.UnderReplicatedChunks))
		dpStaleReplicas.Set(float64(st.StaleReplicas))
		dpPendingDeletes.Set(float64(st.PendingDeletes))
		dpUnrepairableChunks.Set(float64(st.UnrepairableChunks))
	}
}

// SetDataPlaneCoreStats updates under-replicated / pending-delete / unrepairable gauges only.
// Use SetDataPlaneStaleReplicas for godfs_data_stale_replicas so frequent rebalance/GC ticks
// do not reset the stale scan to zero.
func SetDataPlaneCoreStats(underReplicated, pendingDeletes, unrepairable int) {
	if dpUnderReplicatedChunks != nil {
		dpUnderReplicatedChunks.Set(float64(underReplicated))
		dpPendingDeletes.Set(float64(pendingDeletes))
		dpUnrepairableChunks.Set(float64(unrepairable))
	}
}

// SetDataPlaneStaleReplicas sets godfs_data_stale_replicas (checksum mismatch vs metadata).
func SetDataPlaneStaleReplicas(n int) {
	if dpStaleReplicas != nil {
		dpStaleReplicas.Set(float64(n))
	}
}

func IncInFlight(kind InFlightKind) {
	if maintInFlight != nil {
		maintInFlight.WithLabelValues(string(kind)).Inc()
	}
}

func DecInFlight(kind InFlightKind) {
	if maintInFlight != nil {
		maintInFlight.WithLabelValues(string(kind)).Dec()
	}
}

func RecordRebalanceAction(t DataPlaneActionType, err error, reason string) {
	if rebalanceActionsTotal != nil {
		rebalanceActionsTotal.WithLabelValues(string(t)).Inc()
	}
	if err != nil && rebalanceErrorsTotal != nil {
		if reason == "" {
			reason = "error"
		}
		rebalanceErrorsTotal.WithLabelValues(string(t), reason).Inc()
	}
}

func RecordDeleteAction(err error, reason string) {
	if deleteActionsTotal != nil {
		deleteActionsTotal.Inc()
	}
	if err != nil && deleteErrorsTotal != nil {
		if reason == "" {
			reason = "error"
		}
		deleteErrorsTotal.WithLabelValues(reason).Inc()
	}
}

// RecordMaintChecksumRPC counts underlying ChecksumChunk calls from the maintenance verifier wrapper.
func RecordMaintChecksumRPC(success bool) {
	if maintChecksumRPCTotal == nil {
		return
	}
	if success {
		maintChecksumRPCTotal.WithLabelValues("ok").Inc()
	} else {
		maintChecksumRPCTotal.WithLabelValues("error").Inc()
	}
}

// RecordMaintReplicaMetaCompare records replica-vs-metadata digest comparison outcomes
// (match, mismatch, rpc_error, short_checksum).
func RecordMaintReplicaMetaCompare(result string) {
	if maintReplicaMetaCompareTotal != nil {
		maintReplicaMetaCompareTotal.WithLabelValues(result).Inc()
	}
}

func mustRegisterGauge(g prometheus.Gauge) prometheus.Gauge {
	if err := prometheus.Register(g); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			if v, ok := are.ExistingCollector.(prometheus.Gauge); ok {
				return v
			}
		}
	}
	return g
}

func mustRegisterGaugeVec(g *prometheus.GaugeVec) *prometheus.GaugeVec {
	if err := prometheus.Register(g); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			if v, ok := are.ExistingCollector.(*prometheus.GaugeVec); ok {
				return v
			}
		}
	}
	return g
}

func mustRegisterCounter(c prometheus.Counter) prometheus.Counter {
	if err := prometheus.Register(c); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			if v, ok := are.ExistingCollector.(prometheus.Counter); ok {
				return v
			}
		}
	}
	return c
}

func mustRegisterCounterVec(c *prometheus.CounterVec) *prometheus.CounterVec {
	if err := prometheus.Register(c); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			if v, ok := are.ExistingCollector.(*prometheus.CounterVec); ok {
				return v
			}
		}
	}
	return c
}
