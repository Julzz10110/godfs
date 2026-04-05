package placement

import (
	"sort"

	"godfs/internal/domain"
)

// Pick chooses k distinct chunk servers maximizing free capacity (CapacityBytes − usedBytes).
// Nodes with CapacityBytes <= 0 are treated as having unlimited capacity for sorting purposes.
// When several nodes have the same free space, tie-break uses round-robin over registration order:
// sort key (regIndex + rr) mod len(nodes) ascending; rr should be incremented by the caller after each placement.
func Pick(nodes []domain.ChunkNode, k int, usedBytes map[domain.NodeID]int64, rr int) ([]domain.ChunkNode, error) {
	if k <= 0 {
		k = 1
	}
	if len(nodes) < k {
		return nil, domain.ErrInsufficientChunkServers
	}

	lenN := len(nodes)
	type scored struct {
		n      domain.ChunkNode
		free   int64
		regIdx int
	}
	list := make([]scored, 0, lenN)
	for i, n := range nodes {
		used := usedBytes[n.ID]
		cap := n.CapacityBytes
		if cap <= 0 {
			cap = 1 << 62 // treat as "very large"
		}
		free := cap - used
		if free < 0 {
			free = 0
		}
		list = append(list, scored{n: n, free: free, regIdx: i})
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].free != list[j].free {
			return list[i].free > list[j].free
		}
		ti := (list[i].regIdx + rr) % lenN
		tj := (list[j].regIdx + rr) % lenN
		if ti != tj {
			return ti < tj
		}
		return list[i].n.ID < list[j].n.ID
	})

	out := make([]domain.ChunkNode, k)
	for i := 0; i < k; i++ {
		out[i] = list[i].n
	}
	return out, nil
}
