//go:build linux

package main

import (
	"path"
	"sync"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"

	"godfs/pkg/client"
)

// metaPathCache holds short-lived Stat / ListDir / negative lookup entries to cut RPC load.
type metaPathCache struct {
	mu sync.RWMutex

	statTTL time.Duration
	negTTL  time.Duration
	dirTTL  time.Duration

	stat map[string]statCacheEntry
	neg  map[string]time.Time
	dir  map[string]dirCacheEntry
}

type statCacheEntry struct {
	until time.Time
	info  client.FileInfo
}

type dirCacheEntry struct {
	until time.Time
	ents  []fuse.DirEntry
}

func newMetaPathCache(statTTL, negTTL, dirTTL time.Duration) *metaPathCache {
	if statTTL < time.Millisecond {
		statTTL = 2 * time.Second
	}
	if negTTL < time.Millisecond {
		negTTL = statTTL / 2
		if negTTL < time.Millisecond {
			negTTL = time.Second
		}
	}
	if dirTTL < time.Millisecond {
		dirTTL = statTTL
	}
	return &metaPathCache{
		statTTL: statTTL,
		negTTL:  negTTL,
		dirTTL:  dirTTL,
		stat:    map[string]statCacheEntry{},
		neg:     map[string]time.Time{},
		dir:     map[string]dirCacheEntry{},
	}
}

func (c *metaPathCache) getStat(p string) (client.FileInfo, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.stat[p]
	if !ok || time.Now().After(e.until) {
		return client.FileInfo{}, false
	}
	return e.info, true
}

func (c *metaPathCache) getNeg(p string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	t, ok := c.neg[p]
	return ok && time.Now().Before(t)
}

func (c *metaPathCache) getDir(p string) ([]fuse.DirEntry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.dir[p]
	if !ok || time.Now().After(e.until) {
		return nil, false
	}
	out := make([]fuse.DirEntry, len(e.ents))
	copy(out, e.ents)
	return out, true
}

func (c *metaPathCache) putStat(p string, info *client.FileInfo) {
	if c == nil || info == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.neg, p)
	c.stat[p] = statCacheEntry{until: time.Now().Add(c.statTTL), info: *info}
}

func (c *metaPathCache) putNeg(p string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.stat, p)
	delete(c.dir, p)
	c.neg[p] = time.Now().Add(c.negTTL)
}

func (c *metaPathCache) putDir(p string, ents []fuse.DirEntry) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := make([]fuse.DirEntry, len(ents))
	copy(cp, ents)
	c.dir[p] = dirCacheEntry{until: time.Now().Add(c.dirTTL), ents: cp}
}

func (c *metaPathCache) invalidatePath(p string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.stat, p)
	delete(c.neg, p)
	delete(c.dir, p)
}

// invalidateMutation drops cached metadata for p and directory listing caches for its ancestors.
func (c *metaPathCache) invalidateMutation(p string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.stat, p)
	delete(c.neg, p)
	delete(c.dir, p)
	for d := parentDir(p); ; {
		delete(c.dir, d)
		if d == "/" {
			break
		}
		d = parentDir(d)
	}
}

func parentDir(p string) string {
	if p == "" || p == "/" {
		return "/"
	}
	d := path.Dir(p)
	if d == "" {
		return "/"
	}
	return d
}
