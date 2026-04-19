package main

import (
	"context"
	"time"

	"google.golang.org/grpc"

	godfsv1 "godfs/api/proto/godfs/v1"
	"godfs/internal/adapter/repository/metadata"
	"godfs/internal/raftmeta"
	"godfs/internal/security"
)

// maintenanceLoopConfig holds intervals and limits for background rebalance, delete-GC, and orphan chunk cleanup.
type maintenanceLoopConfig struct {
	rebalanceEvery        time.Duration
	rebalanceMaxPerTick   int
	rebalanceMaxAttempts  int
	rebalanceBackoffBase  time.Duration
	rebalanceBackoffMax   time.Duration
	gcEvery               time.Duration
	gcMaxPerTick          int
	gcMaxAttempts         int
	gcBaseBackoff         time.Duration
	gcMaxBackoff          time.Duration
	orphanEvery           time.Duration
	orphanMinAge          time.Duration
	orphanMaxPerNode      int
}

// startRaftBackgroundMaintenance runs periodic rebalance, best-effort chunk delete after metadata removal, and orphan file cleanup on the Raft leader.
func startRaftBackgroundMaintenance(rstore *raftmeta.Service, cfg maintenanceLoopConfig) {
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
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					err = rstore.ExecuteRebalance(ctx, act)
					cancel()
					if err == nil {
						uctx, ucancel := context.WithTimeout(context.Background(), 5*time.Second)
						_ = rstore.ClearRebalanceTask(uctx, act.ChunkID)
						ucancel()
						continue
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
					next := now.Add(backoff).Unix()
					uctx, ucancel := context.WithTimeout(context.Background(), 5*time.Second)
					_ = rstore.MarkRebalanceAttempt(uctx, act.ChunkID, attempts+1, next, err.Error())
					ucancel()
				}
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
				for i := 0; i < cfg.gcMaxPerTick; i++ {
					cid, addr, attempts, ok := rstore.PlanDeleteGC(now)
					if !ok {
						break
					}
					if attempts >= cfg.gcMaxAttempts {
						uctx, ucancel := context.WithTimeout(context.Background(), 5*time.Second)
						_ = rstore.ClearPendingDeleteAddr(uctx, cid, addr)
						ucancel()
						continue
					}
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
					cancel()
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
					next := now.Add(backoff).Unix()
					uctx, ucancel := context.WithTimeout(context.Background(), 5*time.Second)
					_ = rstore.MarkPendingDeleteAttempt(uctx, cid, addr, attempts+1, next)
					ucancel()
				}
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
}

// startSingleMasterBackgroundMaintenance runs the same background tasks for the in-memory metadata store (no Raft leader check).
func startSingleMasterBackgroundMaintenance(m *metadata.Store, cfg maintenanceLoopConfig) {
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
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					err = m.ExecuteRebalance(ctx, act)
					cancel()
					if err == nil {
						m.ClearRebalanceTask(act.ChunkID)
						continue
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
					next := now.Add(backoff).Unix()
					m.MarkRebalanceAttempt(act.ChunkID, attempts+1, next, err.Error())
				}
			}
		}()
	}

	if cfg.gcEvery > 0 {
		go func() {
			t := time.NewTicker(cfg.gcEvery)
			defer t.Stop()
			for range t.C {
				now := time.Now().UTC()
				for i := 0; i < cfg.gcMaxPerTick; i++ {
					cid, addr, attempts, ok := m.PlanDeleteGC(now)
					if !ok {
						break
					}
					if attempts >= cfg.gcMaxAttempts {
						m.ClearPendingDeleteAddr(cid, addr)
						continue
					}
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
					cancel()
					if err == nil {
						m.ClearPendingDeleteAddr(cid, addr)
						continue
					}
					backoff := cfg.gcBaseBackoff * time.Duration(1<<min(attempts, 10))
					if backoff > cfg.gcMaxBackoff {
						backoff = cfg.gcMaxBackoff
					}
					next := now.Add(backoff).Unix()
					m.MarkPendingDeleteAttempt(cid, addr, attempts+1, next)
				}
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
}
