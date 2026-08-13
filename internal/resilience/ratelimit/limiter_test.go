package ratelimit

import (
	"testing"
	"time"
)

func TestLimiter_AllowsUpToBurstThenBlocks(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	l := New(1, 3) // 1 token/sec, burst 3
	l.now = func() time.Time { return now }

	for i := 0; i < 3; i++ {
		if !l.Allow("k") {
			t.Fatalf("Allow() call %d within burst = false, want true", i+1)
		}
	}
	if l.Allow("k") {
		t.Fatal("Allow() beyond burst = true, want false")
	}
}

func TestLimiter_RefillsOverTime(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	l := New(1, 1) // 1 token/sec, burst 1
	l.now = func() time.Time { return now }

	if !l.Allow("k") {
		t.Fatal("first Allow() = false, want true")
	}
	if l.Allow("k") {
		t.Fatal("immediate second Allow() = true, want false (bucket empty)")
	}

	now = now.Add(1100 * time.Millisecond)
	if !l.Allow("k") {
		t.Fatal("Allow() after refill window = false, want true")
	}
}

func TestLimiter_KeysAreIndependent(t *testing.T) {
	l := New(1, 1)
	if !l.Allow("a") {
		t.Fatal("Allow(a) = false, want true")
	}
	if !l.Allow("b") {
		t.Fatal("Allow(b) should be independent of key a's consumption")
	}
}
