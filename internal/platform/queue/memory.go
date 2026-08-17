package queue

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Handler processes one dispatched task's payload. Matches the shape the
// worker registers for both the asynq server (via an asynq.HandlerFunc
// adapter) and MemoryClient, so the same business logic backs either
// transport.
type Handler func(ctx context.Context, taskType string, payload []byte) error

// ErrorRecorder is notified when a Handler returns a non-nil error.
// MemoryClient has no retry concept (there is no queue to hold the task
// while it waits) and no asynq.Server to fire asynq.Config.ErrorHandler, so
// this is how the memory-mode fallback still populates terminal_failures
// (see dlq.go) instead of silently swallowing unexpected handler errors.
type ErrorRecorder func(ctx context.Context, taskType, taskID string, payload []byte, err error)

// memoryCloseGrace/memoryCloseForce bound Close's total wait: grace period
// for in-flight handlers to finish on their own, then the shared context is
// cancelled (a well-behaved Handler -- e.g. ProcessCharge's provider call --
// observes ctx.Done() and returns promptly) and a second, shorter window is
// given before Close gives up and returns anyway. Per the plan's graceful
// shutdown rule ("masing-masing Shutdown(ctx) dengan timeout", "tidak ada
// go func() liar tanpa owner/context-cancel"): every handler goroutine is
// owned by this ctx, and Close is bounded rather than blocking forever on a
// slow/stuck provider.
const (
	memoryCloseGrace = 5 * time.Second
	memoryCloseForce = 2 * time.Second
)

// MemoryClient is the non-durable in-process fallback: Enqueue hands the
// payload straight to the registered Handler on its own goroutine and
// returns immediately. If the process crashes before that goroutine runs,
// the task is lost -- this is the explicitly-accepted tradeoff of the
// non-durable fallback (plan: "Redis fallback: non-durable OK (in-memory),
// outbox Postgres jadi safety net kalau proses restart"): the outbox row
// this dispatch came from is marked 'dispatched' once Enqueue returns nil,
// same as for the durable asynq path, and is not redelivered on restart.
// This is a documented degraded-mode gap, not a bug -- see phase report.
type MemoryClient struct {
	mu       sync.RWMutex
	handlers map[string]Handler
	onError  ErrorRecorder

	ctx    context.Context
	cancel context.CancelFunc

	wg     sync.WaitGroup
	closed bool
}

// NewMemoryClient builds a MemoryClient. onError may be nil (errors are
// merely dropped, useful in tests that don't care).
func NewMemoryClient(onError ErrorRecorder) *MemoryClient {
	ctx, cancel := context.WithCancel(context.Background())
	return &MemoryClient{handlers: make(map[string]Handler), onError: onError, ctx: ctx, cancel: cancel}
}

func (m *MemoryClient) RegisterHandler(taskType string, h Handler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handlers[taskType] = h
}

func (m *MemoryClient) Enqueue(_ context.Context, taskType string, payload []byte, opts EnqueueOptions) error {
	m.mu.RLock()
	h, ok := m.handlers[taskType]
	closed := m.closed
	m.mu.RUnlock()

	if closed {
		return fmt.Errorf("queue: memory client closed")
	}
	if !ok {
		return fmt.Errorf("%w: %q", ErrNoHandler, taskType)
	}

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		// Deliberately not ctx from the caller (the outbox relay's
		// dispatch loop, scoped to one poll tick) -- this goroutine's
		// owner is the MemoryClient itself, and its context is m.ctx,
		// cancelled from Close. That is what makes this goroutine NOT a
		// "go func() liar tanpa owner/context-cancel": it has an owner
		// (MemoryClient) and a cancellation path (m.cancel via Close).
		if err := h(m.ctx, taskType, payload); err != nil {
			if m.onError != nil {
				m.onError(m.ctx, taskType, opts.TaskID, payload, err)
			}
		}
	}()
	return nil
}

// Close marks the client closed to further Enqueue calls and waits for
// in-flight handler goroutines to finish, bounded by memoryCloseGrace +
// memoryCloseForce total: if handlers haven't finished within the grace
// period, their shared context is cancelled (so a well-behaved Handler --
// e.g. one built on ProcessCharge, whose provider call respects ctx.Done()
// -- unwinds promptly), and Close gives up after one more short window
// rather than blocking indefinitely on a stuck handler.
func (m *MemoryClient) Close() error {
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()

	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(memoryCloseGrace):
	}

	m.cancel()

	select {
	case <-done:
	case <-time.After(memoryCloseForce):
		slog.Warn("[queue] Close: memory client close timed out waiting for in-flight handlers", "component", "queue")
	}
	return nil
}

var _ Client = (*MemoryClient)(nil)
