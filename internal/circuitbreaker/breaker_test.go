package circuitbreaker

import (
	"testing"
	"time"
)

// TestBreaker_OpensAfterThreshold_ThenHalfOpensAfterCooldown_ThenCloses is
// the Fase 3 mandatory unit test: "circuit breaker state transition".
func TestBreaker_OpensAfterThreshold_ThenHalfOpensAfterCooldown_ThenCloses(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	b := New(3, 10*time.Second)
	b.now = func() time.Time { return now }

	if b.State() != StateClosed {
		t.Fatalf("initial state = %v, want closed", b.State())
	}
	if !b.Allow() {
		t.Fatal("Allow() on fresh closed breaker = false, want true")
	}

	b.RecordFailure()
	b.RecordFailure()
	if b.State() != StateClosed {
		t.Fatalf("state after 2/3 failures = %v, want still closed", b.State())
	}

	b.RecordFailure() // 3rd failure -> threshold reached
	if b.State() != StateOpen {
		t.Fatalf("state after 3/3 failures = %v, want open", b.State())
	}
	if b.Allow() {
		t.Fatal("Allow() while open, before cooldown elapsed = true, want false")
	}

	// Advance time past the cooldown window.
	now = now.Add(11 * time.Second)
	if !b.Allow() {
		t.Fatal("Allow() after cooldown elapsed = false, want true (half_open trial)")
	}
	if b.State() != StateHalfOpen {
		t.Fatalf("state after cooldown-elapsed Allow() = %v, want half_open", b.State())
	}

	b.RecordSuccess()
	if b.State() != StateClosed {
		t.Fatalf("state after half_open success = %v, want closed", b.State())
	}
	if !b.Allow() {
		t.Fatal("Allow() after closing = false, want true")
	}
}

// TestBreaker_HalfOpenFailureReopens verifies a failed trial call while
// half_open reopens the breaker immediately, restarting its cooldown.
func TestBreaker_HalfOpenFailureReopens(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	b := New(1, 5*time.Second)
	b.now = func() time.Time { return now }

	b.RecordFailure() // threshold 1 -> opens immediately
	if b.State() != StateOpen {
		t.Fatalf("state after 1 failure (threshold 1) = %v, want open", b.State())
	}

	now = now.Add(6 * time.Second)
	if !b.Allow() {
		t.Fatal("Allow() after cooldown = false, want true (half_open trial)")
	}
	if b.State() != StateHalfOpen {
		t.Fatalf("state = %v, want half_open", b.State())
	}

	b.RecordFailure() // trial call fails
	if b.State() != StateOpen {
		t.Fatalf("state after half_open failure = %v, want open again", b.State())
	}
	if b.Allow() {
		t.Fatal("Allow() immediately after re-opening = true, want false")
	}
}

// TestRegistry_IsolatesPerProvider verifies breakers are independent per
// provider name -- one provider's failures must never affect another's.
func TestRegistry_IsolatesPerProvider(t *testing.T) {
	r := NewRegistry(1, time.Minute)

	r.Get("stripe").RecordFailure()
	if r.Get("stripe").State() != StateOpen {
		t.Fatal("stripe breaker should be open after 1 failure (threshold 1)")
	}
	if r.Get("mock").State() != StateClosed {
		t.Fatal("mock breaker should be unaffected by stripe's failure")
	}
	if !r.Get("mock").Allow() {
		t.Fatal("mock breaker should still allow calls")
	}
}
