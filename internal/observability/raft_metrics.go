package observability

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	raftOnce sync.Once

	raftIsLeader       prometheus.Gauge
	raftClusterVoters  prometheus.Gauge
	raftClusterServers prometheus.Gauge

	chunkNodesAlive prometheus.Gauge
	chunkNodesDead  prometheus.Gauge
)

// InitRaftSREMetrics registers basic Raft/control-plane SRE metrics.
// Safe to call multiple times.
func InitRaftSREMetrics() {
	raftOnce.Do(func() {
		raftIsLeader = mustRegisterGauge(prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "godfs_raft_is_leader",
			Help: "1 if this master pod is the current Raft leader, 0 otherwise.",
		}))
		raftClusterVoters = mustRegisterGauge(prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "godfs_raft_cluster_voters",
			Help: "Number of voter masters in the current Raft configuration (leader-reported, best-effort).",
		}))
		raftClusterServers = mustRegisterGauge(prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "godfs_raft_cluster_servers",
			Help: "Number of masters in the current Raft configuration (leader-reported, best-effort).",
		}))

		chunkNodesAlive = mustRegisterGauge(prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "godfs_chunk_nodes_alive",
			Help: "Number of chunk nodes considered alive by heartbeat (best-effort).",
		}))
		chunkNodesDead = mustRegisterGauge(prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "godfs_chunk_nodes_dead",
			Help: "Number of chunk nodes considered dead by heartbeat (best-effort).",
		}))
	})
}

type RaftSREStats struct {
	IsLeader       bool
	ClusterServers int
	ClusterVoters  int
}

func SetRaftSREStats(st RaftSREStats) {
	if raftIsLeader == nil {
		return
	}
	if st.IsLeader {
		raftIsLeader.Set(1)
	} else {
		raftIsLeader.Set(0)
	}
	if raftClusterServers != nil {
		raftClusterServers.Set(float64(st.ClusterServers))
	}
	if raftClusterVoters != nil {
		raftClusterVoters.Set(float64(st.ClusterVoters))
	}
}

type ChunkNodesSREStats struct {
	Alive int
	Dead  int
}

func SetChunkNodesSREStats(st ChunkNodesSREStats) {
	if chunkNodesAlive == nil {
		return
	}
	chunkNodesAlive.Set(float64(st.Alive))
	chunkNodesDead.Set(float64(st.Dead))
}
