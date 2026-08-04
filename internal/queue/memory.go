package queue

import (
	"context"
	"fmt"
	"sync"
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

	wg     sync.WaitGroup
	closed bool
}

// NewMemoryClient builds a MemoryClient. onError may be nil (errors are
// merely dropped, useful in tests that don't care).
func NewMemoryClient(onError ErrorRecorder) *MemoryClient {
	return &MemoryClient{handlers: make(map[string]Handler), onError: onError}
}

// RegisterHandler wires h to process every task of the given type.
func (m *MemoryClient) RegisterHandler(taskType string, h Handler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handlers[taskType] = h
}

func (m *MemoryClient) Enqueue(ctx context.Context, taskType string, payload []byte, opts EnqueueOptions) error {
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
		// Deliberately not ctx from the caller: the caller here is the
		// outbox relay's dispatch loop, whose context is scoped to one
		// poll tick, not to the lifetime of the task it just handed off.
		runCtx := context.Background()
		if err := h(runCtx, taskType, payload); err != nil {
			if m.onError != nil {
				m.onError(runCtx, taskType, opts.TaskID, payload, err)
			}
		}
	}()
	return nil
}

// Close waits for in-flight handler goroutines to finish and marks the
// client closed to further Enqueue calls. Callers bound this with their own
// shutdown timeout (see lifecycle.RunnerActor's interrupt path); Close
// itself does not take a context because sync.WaitGroup has no
// context-aware Wait.
func (m *MemoryClient) Close() error {
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	m.wg.Wait()
	return nil
}

var _ Client = (*MemoryClient)(nil)
