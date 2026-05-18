package main

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"

	godfsv1 "godfs/api/proto/godfs/v1"
	"godfs/internal/adapter/repository/metadata"
	"godfs/internal/domain"
	"godfs/internal/maintenance/checksumcache"
	"godfs/internal/maintenance/limits"
	"godfs/internal/observability"
	"godfs/internal/raftmeta"
	"godfs/internal/security"
)

// maintenanceLoopConfig holds intervals and limits for background rebalance, delete-GC, and orphan chunk cleanup.
type maintenanceLoopConfig struct {
	rebalanceEvery             time.Duration
	rebalanceMaxPerTick        int
	rebalanceMaxAttempts       int
	rebalanceBackoffBase       time.Duration
	rebalanceBackoffMax        time.Duration
	rebalanceBackoffJitterFrac float64
	gcEvery                    time.Duration
	gcMaxPerTick               int
	gcMaxAttempts              int
	gcBaseBackoff              time.Duration
	gcMaxBackoff               time.Duration
	gcBackoffJitterFrac        float64
	orphanEvery                time.Duration
	orphanMinAge               time.Duration
	orphanMaxPerNode           int

	// In-flight limits (0 = disabled).
	rebalanceInFlight       int
	gcInFlight              int
	checksumInFlight        int
	perNodePullInFlight     int
	perNodeChecksumInFlight int

	// Stale replica gauge: periodic full checksum scan (0 = disabled).
	staleReplicaGaugeEvery   time.Duration
	staleReplicaGaugeTimeout time.Duration

	// maintChecksumMaxQPS caps ChecksumChunk RPC rate from maintenance (0 = unlimited).
	maintChecksumMaxQPS float64

	// gcStrict keeps pending deletes after max attempts instead of abandoning them (GODFS_GC_STRICT).
	gcStrict bool
}

// startRaftBackgroundMaintenance runs periodic rebalance, best-effort chunk delete after metadata removal, and orphan file cleanup on the Raft leader.
func startRaftBackgroundMaintenance(rstore *raftmeta.Service, cfg maintenanceLoopConfig) {
	lim := limits.New(limits.Config{
		GlobalInFlight: map[limits.Kind]int{
			limits.KindPullChunk:   cfg.rebalanceInFlight,
			limits.KindDeleteChunk: cfg.gcInFlight,
			limits.KindChecksum:    cfg.checksumInFlight,
		},
		PerKeyInFlight: map[limits.Kind]int{
			limits.KindPullChunk: cfg.perNodePullInFlight,
			limits.KindChecksum:  cfg.perNodeChecksumInFlight,
		},
	})
	ckCache := checksumcache.New(2 * time.Second)
	inner := func(ctx context.Context, addr string, chunkID domain.ChunkID) ([]byte, error) {
		now := time.Now().UTC()
		key := fmt.Sprintf("%s|%s", addr, chunkID)
		if sum, ok := ckCache.Get(key, now); ok {
			return sum, nil
		}
		cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		release, ok := lim.Acquire(cctx, limits.KindChecksum, addr)
		if !ok {
			return nil, cctx.Err()
		}
		observability.IncInFlight(observability.InFlightChecksum)
		defer observability.DecInFlight(observability.InFlightChecksum)
		defer release()

		dopts, err := security.ClientDialOptions()
		if err != nil {
			return nil, err
		}
		cc, err := grpc.NewClient(addr, dopts...)
		if err != nil {
			return nil, err
		}
		defer cc.Close()
		cli := godfsv1.NewChunkServiceClient(cc)
		resp, err := cli.ChecksumChunk(cctx, &godfsv1.ChecksumChunkRequest{ChunkId: string(chunkID)})
		if err != nil {
			return nil, err
		}
		if len(resp.ChecksumSha256) == 32 {
			ckCache.Put(key, resp.ChecksumSha256, now)
		}
		return resp.ChecksumSha256, nil
	}
	rstore.SetChecksumVerifier(wrapMaintChecksumVerifier(inner, cfg.maintChecksumMaxQPS))

	if cfg.rebalanceEvery > 0 {
		go func() {
			t := time.NewTicker(cfg.rebalanceEvery)
			defer t.Stop()
			for range t.C {
				if !rstore.IsLeader() {
					continue
				}
				now := time.Now().UTC()
				for i := 0; i < cfg.rebalanceMaxPerTick; i++ {
					act, err := rstore.PlanRebalance(now)
					if err != nil || act == nil {
						break
					}
					if act.Unrepairable {
						attempts := rstore.RebalanceAttempts(act.ChunkID)
						backoff := cfg.rebalanceBackoffMax
						if backoff <= 0 {
							backoff = 30 * time.Second
						}
						backoff = backoffWithJitter(backoff, cfg.rebalanceBackoffJitterFrac)
						next := now.Add(backoff).Unix()
						uctx, ucancel := context.WithTimeout(context.Background(), 5*time.Second)
						_ = rstore.MarkRebalanceAttempt(uctx, act.ChunkID, attempts+1, next, "unrepairable:"+act.UnrepairableReason)
						ucancel()
						observability.RecordRebalanceAction(observability.ActionRepairStale, context.Canceled, "unrepairable")
						continue
					}
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					key := string(act.TargetNodeID)
					release, ok := lim.Acquire(ctx, limits.KindPullChunk, key)
					if !ok {
						cancel()
						break
					}
					observability.IncInFlight(observability.InFlightPull)
					err = rstore.ExecuteRebalance(ctx, act)
					observability.DecInFlight(observability.InFlightPull)
					release()
					cancel()
					if err == nil {
						if act.RepairExisting {
							observability.RecordRebalanceAction(observability.ActionRepairStale, nil, "")
						} else {
							observability.RecordRebalanceAction(observability.ActionAddReplica, nil, "")
						}
						uctx, ucancel := context.WithTimeout(context.Background(), 5*time.Second)
						_ = rstore.ClearRebalanceTask(uctx, act.ChunkID)
						ucancel()
						continue
					}
					if act.RepairExisting {
						observability.RecordRebalanceAction(observability.ActionRepairStale, err, "execute")
					} else {
						observability.RecordRebalanceAction(observability.ActionAddReplica, err, "execute")
					}
					attempts := rstore.RebalanceAttempts(act.ChunkID)
					if attempts >= cfg.rebalanceMaxAttempts {
						uctx, ucancel := context.WithTimeout(context.Background(), 5*time.Second)
						_ = rstore.ClearRebalanceTask(uctx, act.ChunkID)
						ucancel()
						continue
					}
					backoff := cfg.rebalanceBackoffBase * time.Duration(1<<min(attempts, 10))
					if backoff > cfg.rebalanceBackoffMax {
						backoff = cfg.rebalanceBackoffMax
					}
					backoff = backoffWithJitter(backoff, cfg.rebalanceBackoffJitterFrac)
					next := now.Add(backoff).Unix()
					uctx, ucancel := context.WithTimeout(context.Background(), 5*time.Second)
					_ = rstore.MarkRebalanceAttempt(uctx, act.ChunkID, attempts+1, next, err.Error())
					ucancel()
				}
				st := rstore.DataPlaneStats(now)
				observability.SetDataPlaneCoreStats(st.UnderReplicatedChunks, st.PendingDeletes, st.UnrepairableChunks)
				observability.SetMaintQueueDepth(st.RebalanceQueueDepth, st.GCQueuedChunks)
				f, d, c, b := rstore.NamespaceSnapshot()
				observability.SetNamespaceSnapshot(f, d, c, b)
				observability.SetChunkNodesSREStats(observability.ChunkNodesSREStats{
					Alive: st.ChunkNodesAlive,
					Dead:  st.ChunkNodesDead,
				})
			}
		}()
	}

	if cfg.gcEvery > 0 {
		go func() {
			t := time.NewTicker(cfg.gcEvery)
			defer t.Stop()
			for range t.C {
				if !rstore.IsLeader() {
					continue
				}
				now := time.Now().UTC()
				rstore.PurgeExpiredSoftDeletes(now)
				for i := 0; i < cfg.gcMaxPerTick; i++ {
					cid, addr, attempts, ok := rstore.PlanDeleteGC(now)
					if !ok {
						break
					}
					if attempts >= cfg.gcMaxAttempts {
						if gcAbandonOnMaxAttempts(cfg.gcStrict) {
							uctx, ucancel := context.WithTimeout(context.Background(), 5*time.Second)
							_ = rstore.ClearPendingDeleteAddr(uctx, cid, addr)
							ucancel()
						}
						continue
					}
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					release, ok := lim.Acquire(ctx, limits.KindDeleteChunk, addr)
					if !ok {
						cancel()
						break
					}
					observability.IncInFlight(observability.InFlightDelete)
					err := func() error {
						dopts, err := security.ClientDialOptions()
						if err != nil {
							return err
						}
						cc, err := grpc.NewClient(addr, dopts...)
						if err != nil {
							return err
						}
						defer cc.Close()
						cli := godfsv1.NewChunkServiceClient(cc)
						_, err = cli.DeleteChunk(ctx, &godfsv1.DeleteChunkRequest{ChunkId: string(cid)})
						return err
					}()
					observability.DecInFlight(observability.InFlightDelete)
					release()
					cancel()
					observability.RecordDeleteAction(err, "delete_chunk")
					if err == nil {
						uctx, ucancel := context.WithTimeout(context.Background(), 5*time.Second)
						_ = rstore.ClearPendingDeleteAddr(uctx, cid, addr)
						ucancel()
						continue
					}
					backoff := cfg.gcBaseBackoff * time.Duration(1<<min(attempts, 10))
					if backoff > cfg.gcMaxBackoff {
						backoff = cfg.gcMaxBackoff
					}
					backoff = backoffWithJitter(backoff, cfg.gcBackoffJitterFrac)
					next := now.Add(backoff).Unix()
					uctx, ucancel := context.WithTimeout(context.Background(), 5*time.Second)
					_ = rstore.MarkPendingDeleteAttempt(uctx, cid, addr, attempts+1, next)
					ucancel()
				}
				if cfg.gcStrict && cfg.gcMaxAttempts > 0 {
					observability.SetGCStrictStuck(rstore.CountGCDeleteEntriesAtMaxAttempts(cfg.gcMaxAttempts))
				} else {
					observability.SetGCStrictStuck(0)
				}
				st := rstore.DataPlaneStats(now)
				observability.SetDataPlaneCoreStats(st.UnderReplicatedChunks, st.PendingDeletes, st.UnrepairableChunks)
				observability.SetMaintQueueDepth(st.RebalanceQueueDepth, st.GCQueuedChunks)
				f, d, c, b := rstore.NamespaceSnapshot()
				observability.SetNamespaceSnapshot(f, d, c, b)
				observability.SetChunkNodesSREStats(observability.ChunkNodesSREStats{
					Alive: st.ChunkNodesAlive,
					Dead:  st.ChunkNodesDead,
				})
			}
		}()
	}

	if cfg.orphanEvery > 0 {
		go func() {
			t := time.NewTicker(cfg.orphanEvery)
			defer t.Stop()
			for range t.C {
				if !rstore.IsLeader() {
					continue
				}
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				_ = rstore.OrphanGCOnce(ctx, cfg.orphanMinAge, cfg.orphanMaxPerNode)
				cancel()
			}
		}()
	}

	if cfg.staleReplicaGaugeEvery > 0 {
		go staleReplicaGaugeRaftLoop(rstore, cfg)
	}
}

// startSingleMasterBackgroundMaintenance runs the same background tasks for the in-memory metadata store (no Raft leader check).
func startSingleMasterBackgroundMaintenance(m *metadata.Store, cfg maintenanceLoopConfig) {
	lim := limits.New(limits.Config{
		GlobalInFlight: map[limits.Kind]int{
			limits.KindPullChunk:   cfg.rebalanceInFlight,
			limits.KindDeleteChunk: cfg.gcInFlight,
			limits.KindChecksum:    cfg.checksumInFlight,
		},
		PerKeyInFlight: map[limits.Kind]int{
			limits.KindPullChunk: cfg.perNodePullInFlight,
			limits.KindChecksum:  cfg.perNodeChecksumInFlight,
		},
	})
	ckCache := checksumcache.New(2 * time.Second)
	inner := func(ctx context.Context, addr string, chunkID domain.ChunkID) ([]byte, error) {
		now := time.Now().UTC()
		key := fmt.Sprintf("%s|%s", addr, chunkID)
		if sum, ok := ckCache.Get(key, now); ok {
			return sum, nil
		}
		cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		release, ok := lim.Acquire(cctx, limits.KindChecksum, addr)
		if !ok {
			return nil, cctx.Err()
		}
		observability.IncInFlight(observability.InFlightChecksum)
		defer observability.DecInFlight(observability.InFlightChecksum)
		defer release()

		dopts, err := security.ClientDialOptions()
		if err != nil {
			return nil, err
		}
		cc, err := grpc.NewClient(addr, dopts...)
		if err != nil {
			return nil, err
		}
		defer cc.Close()
		cli := godfsv1.NewChunkServiceClient(cc)
		resp, err := cli.ChecksumChunk(cctx, &godfsv1.ChecksumChunkRequest{ChunkId: string(chunkID)})
		if err != nil {
			return nil, err
		}
		if len(resp.ChecksumSha256) == 32 {
			ckCache.Put(key, resp.ChecksumSha256, now)
		}
		return resp.ChecksumSha256, nil
	}
	m.SetChecksumVerifier(wrapMaintChecksumVerifier(inner, cfg.maintChecksumMaxQPS))

	if cfg.rebalanceEvery > 0 {
		go func() {
			t := time.NewTicker(cfg.rebalanceEvery)
			defer t.Stop()
			for range t.C {
				now := time.Now().UTC()
				for i := 0; i < cfg.rebalanceMaxPerTick; i++ {
					act, err := m.PlanRebalance(now)
					if err != nil || act == nil {
						break
					}
					if act.Unrepairable {
						attempts := m.RebalanceAttempts(act.ChunkID)
						backoff := cfg.rebalanceBackoffMax
						if backoff <= 0 {
							backoff = 30 * time.Second
						}
						backoff = backoffWithJitter(backoff, cfg.rebalanceBackoffJitterFrac)
						next := now.Add(backoff).Unix()
						m.MarkRebalanceAttempt(act.ChunkID, attempts+1, next, "unrepairable:"+act.UnrepairableReason)
						observability.RecordRebalanceAction(observability.ActionRepairStale, context.Canceled, "unrepairable")
						continue
					}
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					key := string(act.TargetNodeID)
					release, ok := lim.Acquire(ctx, limits.KindPullChunk, key)
					if !ok {
						cancel()
						break
					}
					observability.IncInFlight(observability.InFlightPull)
					err = m.ExecuteRebalance(ctx, act)
					observability.DecInFlight(observability.InFlightPull)
					release()
					cancel()
					if err == nil {
						if act.RepairExisting {
							observability.RecordRebalanceAction(observability.ActionRepairStale, nil, "")
						} else {
							observability.RecordRebalanceAction(observability.ActionAddReplica, nil, "")
						}
						m.ClearRebalanceTask(act.ChunkID)
						continue
					}
					if act.RepairExisting {
						observability.RecordRebalanceAction(observability.ActionRepairStale, err, "execute")
					} else {
						observability.RecordRebalanceAction(observability.ActionAddReplica, err, "execute")
					}
					attempts := m.RebalanceAttempts(act.ChunkID)
					if attempts >= cfg.rebalanceMaxAttempts {
						m.ClearRebalanceTask(act.ChunkID)
						continue
					}
					backoff := cfg.rebalanceBackoffBase * time.Duration(1<<min(attempts, 10))
					if backoff > cfg.rebalanceBackoffMax {
						backoff = cfg.rebalanceBackoffMax
					}
					backoff = backoffWithJitter(backoff, cfg.rebalanceBackoffJitterFrac)
					next := now.Add(backoff).Unix()
					m.MarkRebalanceAttempt(act.ChunkID, attempts+1, next, err.Error())
				}
				st := m.DataPlaneStats(now)
				observability.SetDataPlaneCoreStats(st.UnderReplicatedChunks, st.PendingDeletes, st.UnrepairableChunks)
				observability.SetMaintQueueDepth(st.RebalanceQueueDepth, st.GCQueuedChunks)
				f, d, c, b := m.NamespaceSnapshot()
				observability.SetNamespaceSnapshot(f, d, c, b)
				observability.SetChunkNodesSREStats(observability.ChunkNodesSREStats{
					Alive: st.ChunkNodesAlive,
					Dead:  st.ChunkNodesDead,
				})
			}
		}()
	}

	if cfg.gcEvery > 0 {
		go func() {
			t := time.NewTicker(cfg.gcEvery)
			defer t.Stop()
			for range t.C {
				now := time.Now().UTC()
				m.PurgeExpiredSoftDeletes(now)
				for i := 0; i < cfg.gcMaxPerTick; i++ {
					cid, addr, attempts, ok := m.PlanDeleteGC(now)
					if !ok {
						break
					}
					if attempts >= cfg.gcMaxAttempts {
						if gcAbandonOnMaxAttempts(cfg.gcStrict) {
							m.ClearPendingDeleteAddr(cid, addr)
						}
						continue
					}
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					release, ok := lim.Acquire(ctx, limits.KindDeleteChunk, addr)
					if !ok {
						cancel()
						break
					}
					observability.IncInFlight(observability.InFlightDelete)
					err := func() error {
						dopts, err := security.ClientDialOptions()
						if err != nil {
							return err
						}
						cc, err := grpc.NewClient(addr, dopts...)
						if err != nil {
							return err
						}
						defer cc.Close()
						cli := godfsv1.NewChunkServiceClient(cc)
						_, err = cli.DeleteChunk(ctx, &godfsv1.DeleteChunkRequest{ChunkId: string(cid)})
						return err
					}()
					observability.DecInFlight(observability.InFlightDelete)
					release()
					cancel()
					observability.RecordDeleteAction(err, "delete_chunk")
					if err == nil {
						m.ClearPendingDeleteAddr(cid, addr)
						continue
					}
					backoff := cfg.gcBaseBackoff * time.Duration(1<<min(attempts, 10))
					if backoff > cfg.gcMaxBackoff {
						backoff = cfg.gcMaxBackoff
					}
					backoff = backoffWithJitter(backoff, cfg.gcBackoffJitterFrac)
					next := now.Add(backoff).Unix()
					m.MarkPendingDeleteAttempt(cid, addr, attempts+1, next)
				}
				if cfg.gcStrict && cfg.gcMaxAttempts > 0 {
					observability.SetGCStrictStuck(m.CountGCDeleteEntriesAtMaxAttempts(cfg.gcMaxAttempts))
				} else {
					observability.SetGCStrictStuck(0)
				}
				st := m.DataPlaneStats(now)
				observability.SetDataPlaneCoreStats(st.UnderReplicatedChunks, st.PendingDeletes, st.UnrepairableChunks)
				observability.SetMaintQueueDepth(st.RebalanceQueueDepth, st.GCQueuedChunks)
				f, d, c, b := m.NamespaceSnapshot()
				observability.SetNamespaceSnapshot(f, d, c, b)
				observability.SetChunkNodesSREStats(observability.ChunkNodesSREStats{
					Alive: st.ChunkNodesAlive,
					Dead:  st.ChunkNodesDead,
				})
			}
		}()
	}

	if cfg.orphanEvery > 0 {
		go func() {
			t := time.NewTicker(cfg.orphanEvery)
			defer t.Stop()
			for range t.C {
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				_ = m.OrphanGCOnce(ctx, cfg.orphanMinAge, cfg.orphanMaxPerNode)
				cancel()
			}
		}()
	}

	if cfg.staleReplicaGaugeEvery > 0 {
		go staleReplicaGaugeMemoryLoop(m, cfg)
	}
}

func staleReplicaGaugeRaftLoop(rstore *raftmeta.Service, cfg maintenanceLoopConfig) {
	t := time.NewTicker(cfg.staleReplicaGaugeEvery)
	defer t.Stop()
	for range t.C {
		if !rstore.IsLeader() {
			continue
		}
		now := time.Now().UTC()
		timeout := cfg.staleReplicaGaugeTimeout
		if timeout <= 0 {
			timeout = 2 * time.Minute
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		n := rstore.CountStaleReplicas(ctx, now)
		cancel()
		observability.SetDataPlaneStaleReplicas(n)
	}
}

func staleReplicaGaugeMemoryLoop(m *metadata.Store, cfg maintenanceLoopConfig) {
	t := time.NewTicker(cfg.staleReplicaGaugeEvery)
	defer t.Stop()
	for range t.C {
		now := time.Now().UTC()
		timeout := cfg.staleReplicaGaugeTimeout
		if timeout <= 0 {
			timeout = 2 * time.Minute
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		n := m.CountStaleReplicas(ctx, now)
		cancel()
		observability.SetDataPlaneStaleReplicas(n)
	}
}
