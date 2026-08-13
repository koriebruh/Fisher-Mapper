package queue

import (
	"context"
	"testing"
	"time"
)

// TestMemoryClient_CloseIsBoundedAndCancelsInFlightHandlers proves the
// graceful-shutdown fix: an in-flight handler that respects ctx.Done() (as
// payment.Service.ProcessCharge's provider call does) unwinds promptly once
// Close cancels the shared context, and Close itself never blocks longer
// than memoryCloseGrace+memoryCloseForce -- there is no unbounded
// sync.WaitGroup.Wait() left over from a stuck/slow handler, which would
// otherwise hang a SIGTERM until the process supervisor force-kills it.
func TestMemoryClient_CloseIsBoundedAndCancelsInFlightHandlers(t *testing.T) {
	c := NewMemoryClient(nil)

	handlerCtxDone := make(chan struct{})
	c.RegisterHandler("slow", func(ctx context.Context, _ string, _ []byte) error {
		<-ctx.Done() // a well-behaved handler: blocks until told to stop.
		close(handlerCtxDone)
		return ctx.Err()
	})

	if err := c.Enqueue(context.Background(), "slow", []byte(`{}`), EnqueueOptions{}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Give the handler goroutine a moment to actually start and block on
	// ctx.Done() before we call Close.
	time.Sleep(20 * time.Millisecond)

	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- c.Close() }()

	select {
	case err := <-done:
		elapsed := time.Since(start)
		if err != nil {
			t.Errorf("Close() = %v, want nil", err)
		}
		// Close should not have needed to wait through the full grace
		// period -- cancelling the context should make the handler return
		// almost immediately, well under memoryCloseForce alone.
		if elapsed > memoryCloseGrace+memoryCloseForce {
			t.Fatalf("Close() took %v, want <= %v (bounded)", elapsed, memoryCloseGrace+memoryCloseForce)
		}
	case <-time.After(memoryCloseGrace + memoryCloseForce + time.Second):
		t.Fatal("Close() did not return within its documented bound -- graceful shutdown regressed")
	}

	select {
	case <-handlerCtxDone:
	default:
		t.Error("handler's context was never cancelled/observed as done by Close")
	}
}

// TestMemoryClient_EnqueueAfterCloseFails verifies Enqueue rejects work once
// the client is closed rather than silently spawning an orphaned goroutine.
func TestMemoryClient_EnqueueAfterCloseFails(t *testing.T) {
	c := NewMemoryClient(nil)
	c.RegisterHandler("noop", func(context.Context, string, []byte) error { return nil })

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := c.Enqueue(context.Background(), "noop", []byte(`{}`), EnqueueOptions{}); err == nil {
		t.Fatal("Enqueue after Close = nil error, want rejection")
	}
}
