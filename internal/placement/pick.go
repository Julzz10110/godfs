package placement

import (
	"sort"

	"godfs/internal/domain"
)

// Pick chooses k distinct chunk servers maximizing free capacity (CapacityBytes − usedBytes).
// Nodes with CapacityBytes <= 0 are treated as having unlimited capacity for sorting purposes.
func Pick(nodes []domain.ChunkNode, k int, usedBytes map[domain.NodeID]int64) ([]domain.ChunkNode, error) {
	if k <= 0 {
		k = 1
	}
	if len(nodes) < k {
		return nil, domain.ErrInsufficientChunkServers
	}

	type scored struct {
		n    domain.ChunkNode
		free int64
	}
	list := make([]scored, 0, len(nodes))
	for _, n := range nodes {
		used := usedBytes[n.ID]
		cap := n.CapacityBytes
		if cap <= 0 {
			cap = 1 << 62 // treat as "very large"
		}
		free := cap - used
		if free < 0 {
			free = 0
		}
		list = append(list, scored{n: n, free: free})
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].free != list[j].free {
			return list[i].free > list[j].free
		}
		return list[i].n.ID < list[j].n.ID
	})

	out := make([]domain.ChunkNode, k)
	for i := 0; i < k; i++ {
		out[i] = list[i].n
	}
	return out, nil
}
