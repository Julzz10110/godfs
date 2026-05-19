package checksumcache

import (
	"sync"
	"time"
)

type Cache struct {
	mu  sync.Mutex
	ttl time.Duration
	m   map[string]entry
}

type entry struct {
	sum []byte
	exp time.Time
}

func New(ttl time.Duration) *Cache {
	if ttl <= 0 {
		ttl = 2 * time.Second
	}
	return &Cache{ttl: ttl, m: map[string]entry{}}
}

func (c *Cache) Get(key string, now time.Time) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[key]
	if !ok {
		return nil, false
	}
	if !e.exp.After(now) {
		delete(c.m, key)
		return nil, false
	}
	return append([]byte(nil), e.sum...), true
}

func (c *Cache) Put(key string, sum []byte, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[key] = entry{
		sum: append([]byte(nil), sum...),
		exp: now.Add(c.ttl),
	}
}
