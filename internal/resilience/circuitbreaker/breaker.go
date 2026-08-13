// Package circuitbreaker is a "stub cheap" item from the project plan: a
// small counter+cooldown circuit breaker per provider, so a provider that
// is failing outright doesn't keep getting hammered by every charge task
// that happens to target it. Deliberately simple -- no half-open trial
// concurrency limit, no sliding window -- cheap to build, cheap to delete
// if it turns out not to be needed.
package circuitbreaker

import (
	"sync"
	"time"
)

// State is the breaker's current mode.
type State int

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half_open"
	default:
		return "closed"
	}
}

// Breaker is a single provider's circuit breaker: closed (calls allowed) ->
// open (calls blocked) after failureThreshold consecutive failures -> after
// cooldown elapses, half_open (exactly one trial call allowed) -> closed on
// success or back to open on failure.
type Breaker struct {
	failureThreshold int
	cooldown         time.Duration
	now              func() time.Time

	mu       sync.Mutex
	state    State
	failures int
	openedAt time.Time
}

// New clamps failureThreshold up to 1 if given less (a breaker with zero
// threshold would never admit anything, which is never useful).
func New(failureThreshold int, cooldown time.Duration) *Breaker {
	if failureThreshold < 1 {
		failureThreshold = 1
	}
	return &Breaker{
		failureThreshold: failureThreshold,
		cooldown:         cooldown,
		now:              time.Now,
	}
}

// Allow reports whether a call should proceed. Closed and half_open both
// allow; open blocks until cooldown has elapsed, at which point Allow
// itself transitions the breaker to half_open and allows exactly the call
// that observed the transition.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case StateOpen:
		if b.now().Sub(b.openedAt) >= b.cooldown {
			b.state = StateHalfOpen
			return true
		}
		return false
	default:
		return true
	}
}

// RecordSuccess reports a successful call: closes the breaker and resets
// the failure count, from any prior state (closed staying closed is a
// no-op; half_open succeeding closes; a stray success recorded while open
// -- shouldn't happen since Allow gates calls -- still closes).
func (b *Breaker) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
	b.state = StateClosed
}

// RecordFailure reports a failed call. A failure while half_open reopens
// immediately (the trial call failed). A failure while closed increments
// the counter and opens once failureThreshold is reached.
func (b *Breaker) RecordFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state == StateHalfOpen {
		b.state = StateOpen
		b.openedAt = b.now()
		b.failures = 0
		return
	}

	b.failures++
	if b.failures >= b.failureThreshold {
		b.state = StateOpen
		b.openedAt = b.now()
		b.failures = 0
	}
}

// State returns the breaker's current state, for observability/tests. Does
// NOT perform the open->half_open cooldown check Allow does -- it's a pure
// read.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Registry hands out one Breaker per provider name, created lazily with
// shared thresholds.
type Registry struct {
	failureThreshold int
	cooldown         time.Duration

	mu       sync.Mutex
	breakers map[string]*Breaker
}

// NewRegistry builds a Registry; every provider gets its own Breaker
// constructed with the same failureThreshold/cooldown.
func NewRegistry(failureThreshold int, cooldown time.Duration) *Registry {
	return &Registry{
		failureThreshold: failureThreshold,
		cooldown:         cooldown,
		breakers:         make(map[string]*Breaker),
	}
}

// Get returns the Breaker for name, creating it on first use.
func (r *Registry) Get(name string) *Breaker {
	r.mu.Lock()
	defer r.mu.Unlock()
	b, ok := r.breakers[name]
	if !ok {
		b = New(r.failureThreshold, r.cooldown)
		r.breakers[name] = b
	}
	return b
}
