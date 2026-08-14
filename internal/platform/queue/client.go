// Package queue owns task dispatch: the asynq (Redis-backed, durable)
// client, an in-process memory fallback (non-durable -- the Postgres outbox
// is the safety net per the plan's explicit decision), and a Client that
// switches between them based on live Redis health.
//
// Per the Fase 3 "Catatan desain wajib": this package's Enqueue is the
// *dispatch* half of the outbox pattern, which is safe to retry (that's the
// whole point of the deterministic TaskID below). It has no opinion on
// provider calls at all -- that invariant lives in the payment state
// machine's pending->processing compare-and-swap, one layer up
// (payment.Service.ProcessCharge), never here.
package queue

import (
	"context"
	"errors"
	"fmt"

	"github.com/hibiken/asynq"
)

// TaskTypeCharge identifies the "create charge" task: the one task type
// that must never be auto-retried by the queue (plan item 12 / Fase 3
// invariant). Defined here (not in package payment) so outbox/relay can
// branch on it without importing the payment package -- avoiding a
// payment<->outbox<->queue import cycle, since payment already imports this
// package for the constant when building outbox rows.
const TaskTypeCharge = "charge"

// TaskTypeRefund identifies the "process refund" task (Fase 4): a provider
// call exactly like charge, subject to the identical no-auto-retry
// invariant -- once dispatched, retrying it blindly risks a double refund,
// so it gets the same MaxRetry(0) treatment in outbox.Relay.dispatchOne.
const TaskTypeRefund = "refund"

// TaskTypePayout identifies the "process payout" task: a provider call that
// disburses money OUT, standalone (not tied to an existing charge). Subject
// to the identical no-auto-retry invariant as charge/refund -- a redelivered
// payout task must never call the provider a second time, or it becomes a
// double disbursement.
const TaskTypePayout = "payout"

// EnqueueOptions carries the per-dispatch options a Client.Enqueue call
// needs. MaxRetry, when non-nil, overrides the queue's default retry count
// for this one task -- the relay sets it to 0 for TaskTypeCharge and leaves
// it nil (server default) for everything else, per plan: "charge task
// no-auto-retry, task lain retry normal".
type EnqueueOptions struct {
	// TaskID, if non-empty, is used as asynq's deterministic dedup key so
	// re-dispatching the same outbox row twice (e.g. after a relay crash
	// between enqueue and commit) enqueues at most one live task.
	TaskID string

	// MaxRetry overrides the default retry count for this task if non-nil.
	MaxRetry *int

	// QueueName, if non-empty, routes the task to that asynq queue instead
	// of asynq's implicit "default" queue. Ignored by MemoryClient (a
	// single in-process fallback has no queue concept); AsynqClient maps it
	// to asynq.Queue(name). The asynq server consuming this queue must have
	// been started with a matching entry in asynq.Config.Queues, or the
	// task will sit unconsumed -- see cmd/worker/main.go.
	QueueName string
}

// Client dispatches tasks to be processed asynchronously, either durably
// (asynq/Redis) or as an in-process fallback (memory, non-durable).
type Client interface {
	Enqueue(ctx context.Context, taskType string, payload []byte, opts EnqueueOptions) error
	Close() error
}

// NewClient is kept as-is from Fase 1 for the /readyz health check's use
// (checkRedis in transport/rest/health.go) -- unrelated to the Client
// interface above, which AsynqClient wraps separately.
func NewClient(addr string) *asynq.Client {
	return asynq.NewClient(asynq.RedisClientOpt{Addr: addr})
}

func Ping(client *asynq.Client) error {
	if err := client.Ping(); err != nil {
		return fmt.Errorf("queue: ping redis: %w", err)
	}
	return nil
}

// AsynqClient adapts *asynq.Client to the Client interface.
type AsynqClient struct {
	c *asynq.Client
}

func NewAsynqClient(addr string) *AsynqClient {
	return &AsynqClient{c: NewClient(addr)}
}

func (a *AsynqClient) Enqueue(ctx context.Context, taskType string, payload []byte, opts EnqueueOptions) error {
	var taskOpts []asynq.Option
	if opts.TaskID != "" {
		taskOpts = append(taskOpts, asynq.TaskID(opts.TaskID))
	}
	if opts.MaxRetry != nil {
		taskOpts = append(taskOpts, asynq.MaxRetry(*opts.MaxRetry))
	}
	if opts.QueueName != "" {
		taskOpts = append(taskOpts, asynq.Queue(opts.QueueName))
	}

	task := asynq.NewTask(taskType, payload, taskOpts...)
	if _, err := a.c.EnqueueContext(ctx, task); err != nil {
		if errors.Is(err, asynq.ErrTaskIDConflict) {
			// A task with this deterministic ID already exists (queued,
			// in-flight, or completed and not yet swept) -- this dispatch
			// has already happened at least once. Per the design
			// invariant, re-dispatch is idempotent: treat this as success
			// rather than an error the relay would spin on.
			return nil
		}
		return fmt.Errorf("queue: asynq enqueue: %w", err)
	}
	return nil
}

func (a *AsynqClient) Close() error {
	return a.c.Close()
}

var _ Client = (*AsynqClient)(nil)

// ErrNoHandler is returned by MemoryClient.Enqueue when no handler is
// registered for the task type.
var ErrNoHandler = errors.New("queue: no handler registered for task type")
