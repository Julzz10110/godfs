package dataplane

import (
	"sort"

	"godfs/internal/domain"
)

// ChunkReplicaView is the minimal replica view for under-replication listing.
type ChunkReplicaView struct {
	NodeID domain.NodeID
}

// ListUnderReplicated scans chunk metadata and returns entries where live replicas < targetRF.
// pathByChunk maps chunk IDs to referencing file paths (best-effort; may be empty).
func ListUnderReplicated(
	targetRF int,
	isAlive func(domain.NodeID) bool,
	chunks map[domain.ChunkID][]ChunkReplicaView,
	pathByChunk map[domain.ChunkID][]string,
	limit int,
) (entries []domain.UnderReplicatedChunk, total int) {
	if targetRF <= 1 {
		return nil, 0
	}
	if isAlive == nil {
		isAlive = func(domain.NodeID) bool { return true }
	}
	type item struct {
		id domain.ChunkID
	}
	var under []item
	for id, reps := range chunks {
		if len(reps) == 0 {
			continue
		}
		alive := 0
		for _, r := range reps {
			if isAlive(r.NodeID) {
				alive++
			}
		}
		if alive < targetRF {
			under = append(under, item{id: id})
		}
	}
	total = len(under)
	sort.Slice(under, func(i, j int) bool {
		return string(under[i].id) < string(under[j].id)
	})
	if limit <= 0 || limit > total {
		limit = total
	}
	entries = make([]domain.UnderReplicatedChunk, 0, limit)
	for i := 0; i < limit; i++ {
		id := under[i].id
		reps := chunks[id]
		alive := 0
		var dead []string
		for _, r := range reps {
			if isAlive(r.NodeID) {
				alive++
			} else {
				dead = append(dead, string(r.NodeID))
			}
		}
		paths := append([]string(nil), pathByChunk[id]...)
		sort.Strings(paths)
		if len(paths) > 3 {
			paths = paths[:3]
		}
		entries = append(entries, domain.UnderReplicatedChunk{
			ChunkID:           id,
			TargetReplication: targetRF,
			AliveReplicas:     alive,
			TotalReplicas:     len(reps),
			SamplePaths:       paths,
			DeadNodeIDs:       dead,
		})
	}
	return entries, total
}
