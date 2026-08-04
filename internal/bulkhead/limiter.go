// Package bulkhead provides a per-key concurrency limiter: a bounded number
// of "slots" per provider name, so a slow or stuck provider can only ever
// occupy its own capacity and never starves worker capacity meant for other
// providers (plan item 13: "bulkhead (worker pool/queue terpisah per
// provider atau concurrency limiter) biar satu provider lambat gak
// starvin provider lain").
package bulkhead

import (
	"context"
	"sync"
)

// Limiter bounds concurrent in-flight operations per key.
type Limiter struct {
	capacity int

	mu   sync.Mutex
	sems map[string]chan struct{}
}

// New builds a Limiter allowing up to capacity concurrent operations per
// key. capacity < 1 is treated as 1 (a key with zero capacity would never
// admit anything, which is never useful).
func New(capacity int) *Limiter {
	if capacity < 1 {
		capacity = 1
	}
	return &Limiter{capacity: capacity, sems: make(map[string]chan struct{})}
}

func (l *Limiter) semFor(key string) chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	s, ok := l.sems[key]
	if !ok {
		s = make(chan struct{}, l.capacity)
		l.sems[key] = s
	}
	return s
}

// Acquire blocks until a slot for key is free, or ctx is done first.
func (l *Limiter) Acquire(ctx context.Context, key string) error {
	sem := l.semFor(key)
	select {
	case sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release frees one slot for key. Must be called exactly once per
// successful Acquire, typically via defer.
func (l *Limiter) Release(key string) {
	sem := l.semFor(key)
	select {
	case <-sem:
	default:
		// Release without a matching Acquire -- a caller bug, but panicking
		// here would take down a worker over a bookkeeping mistake, so this
		// is deliberately a silent no-op rather than a panic.
	}
}
