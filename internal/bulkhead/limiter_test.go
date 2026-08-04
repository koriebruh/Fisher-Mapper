package bulkhead

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestLimiter_SlowProviderDoesNotStarveFastProvider is the Fase 3
// validation 6 proof, done in-process (per plan: two providers, one slow,
// the other must stay responsive) rather than via a docker-level
// choreography that wouldn't be deterministic: a slow provider saturating
// its own capacity must never block a call scoped to a different key.
func TestLimiter_SlowProviderDoesNotStarveFastProvider(t *testing.T) {
	l := New(1) // capacity 1 per key -- easy to saturate on purpose

	var wg sync.WaitGroup
	blockSlow := make(chan struct{})

	// Occupy the "slow" provider's only slot and hold it until the test
	// says to release, simulating a stuck/slow provider call.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := l.Acquire(context.Background(), "slow"); err != nil {
			t.Errorf("acquire slow: %v", err)
			return
		}
		<-blockSlow
		l.Release("slow")
	}()

	// Give the goroutine above a moment to actually acquire before we
	// measure the "fast" key's latency.
	time.Sleep(20 * time.Millisecond)

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := l.Acquire(ctx, "fast"); err != nil {
		t.Fatalf("acquire fast (should be independent of slow): %v", err)
	}
	l.Release("fast")
	elapsed := time.Since(start)

	close(blockSlow)
	wg.Wait()

	if elapsed > 100*time.Millisecond {
		t.Fatalf("acquiring 'fast' took %v while 'slow' held its slot -- bulkhead did not isolate providers", elapsed)
	}
}

// TestLimiter_SameKeySerializesUpToCapacity verifies the flip side: within
// one key, concurrency really is bounded -- a second Acquire on the same
// key blocks until the first Release.
func TestLimiter_SameKeySerializesUpToCapacity(t *testing.T) {
	l := New(1)

	if err := l.Acquire(context.Background(), "p"); err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := l.Acquire(ctx, "p"); err == nil {
		t.Fatal("second acquire on the same saturated key succeeded, want it to block until release")
	}

	l.Release("p")
	if err := l.Acquire(context.Background(), "p"); err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	l.Release("p")
}
