package limits

import (
	"context"
	"sync"
)

// Kind identifies a class of background maintenance work.
type Kind string

const (
	KindPullChunk   Kind = "pull_chunk"
	KindDeleteChunk Kind = "delete_chunk"
	KindChecksum    Kind = "checksum"
)

// Limiter provides global and per-key in-flight limits.
// It is intentionally simple: best-effort fairness, no persistence, no metrics.
type Limiter struct {
	global map[Kind]chan struct{}

	mu   sync.Mutex
	perK map[Kind]map[string]chan struct{}

	perKeyLimit map[Kind]int
}

type Config struct {
	// GlobalInFlight: 0 disables limiting for this kind.
	GlobalInFlight map[Kind]int
	// PerKeyInFlight: 0 disables per-key limiting for this kind.
	PerKeyInFlight map[Kind]int
}

func New(cfg Config) *Limiter {
	l := &Limiter{
		global:      map[Kind]chan struct{}{},
		perK:        map[Kind]map[string]chan struct{}{},
		perKeyLimit: map[Kind]int{},
	}
	for k, n := range cfg.GlobalInFlight {
		if n > 0 {
			l.global[k] = make(chan struct{}, n)
		}
	}
	for k, n := range cfg.PerKeyInFlight {
		if n > 0 {
			l.perKeyLimit[k] = n
			l.perK[k] = map[string]chan struct{}{}
		}
	}
	return l
}

// Acquire tries to take a slot for kind. key is used for per-key limiting (nodeID, address, etc).
// Returns a release function and ok=false if ctx is canceled before acquiring.
func (l *Limiter) Acquire(ctx context.Context, kind Kind, key string) (release func(), ok bool) {
	var releases []func()
	defer func() {
		if !ok {
			for i := len(releases) - 1; i >= 0; i-- {
				releases[i]()
			}
		}
	}()

	if g := l.global[kind]; g != nil {
		select {
		case g <- struct{}{}:
			releases = append(releases, func() { <-g })
		case <-ctx.Done():
			return nil, false
		}
	}

	if key != "" && l.perKeyLimit[kind] > 0 {
		ch := l.perKeyChan(kind, key)
		select {
		case ch <- struct{}{}:
			releases = append(releases, func() { <-ch })
		case <-ctx.Done():
			return nil, false
		}
	}

	ok = true
	return func() {
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
	}, true
}

func (l *Limiter) perKeyChan(kind Kind, key string) chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	m := l.perK[kind]
	if m == nil {
		return nil
	}
	if ch, ok := m[key]; ok {
		return ch
	}
	ch := make(chan struct{}, l.perKeyLimit[kind])
	m[key] = ch
	return ch
}
