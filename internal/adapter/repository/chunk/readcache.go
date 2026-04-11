package chunk

import (
	"fmt"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
)

// ReadRangeCache is an optional LRU of recently read byte ranges (chunk_id + offset + length).
type ReadRangeCache struct {
	lru      *lru.Cache[string, []byte]
	maxBytes int64
	mu       sync.Mutex
}

// MaxEntryBytes returns the maximum size of a single cached range.
func (r *ReadRangeCache) MaxEntryBytes() int64 {
	if r == nil {
		return 0
	}
	return r.maxBytes
}

// NewReadRangeCache creates a cache with at most entries items; single entry may not exceed maxBytes.
func NewReadRangeCache(entries int, maxBytes int64) (*ReadRangeCache, error) {
	if entries <= 0 || maxBytes <= 0 {
		return nil, nil
	}
	c, err := lru.New[string, []byte](entries)
	if err != nil {
		return nil, err
	}
	return &ReadRangeCache{lru: c, maxBytes: maxBytes}, nil
}

func readCacheKey(chunkID string, offset, length int64) string {
	return fmt.Sprintf("%s@%d@%d", chunkID, offset, length)
}

// Get returns a cached range copy or false.
func (r *ReadRangeCache) Get(chunkID string, offset, length int64) ([]byte, bool) {
	if r == nil || r.lru == nil {
		return nil, false
	}
	if length <= 0 || length > r.maxBytes {
		return nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.lru.Get(readCacheKey(chunkID, offset, length))
	if !ok {
		return nil, false
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, true
}

// Add stores a copy of data if it fits the configured max size.
func (r *ReadRangeCache) Add(chunkID string, offset, length int64, data []byte) {
	if r == nil || r.lru == nil {
		return
	}
	if int64(len(data)) != length || length <= 0 || length > r.maxBytes {
		return
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lru.Add(readCacheKey(chunkID, offset, length), cp)
}
