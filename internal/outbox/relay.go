package outbox

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"time"

	"Fisher-Mapper/internal/queue"
)

// Relay periodically drains pending outbox rows to a queue.Client. It is
// the actor internal/lifecycle.RunnerActor wraps for oklog/run.Group.
type Relay struct {
	store        *Store
	client       queue.Client
	baseInterval time.Duration
	maxInterval  time.Duration
	batchSize    int
}

// NewRelay builds a Relay. baseInterval is the poll cadence while dispatch
// is succeeding; on failures (e.g. Redis unreachable) the loop backs off
// (with jitter) up to maxInterval and resets to baseInterval as soon as a
// batch dispatches cleanly again -- the "backoff+jitter buat retry dispatch"
// plan item, applied at the poll-loop level rather than per-row, since
// dispatch retries are safe/idempotent regardless of pacing (see package
// doc in store.go).
func NewRelay(store *Store, client queue.Client, baseInterval, maxInterval time.Duration, batchSize int) *Relay {
	if baseInterval <= 0 {
		baseInterval = 2 * time.Second
	}
	if maxInterval < baseInterval {
		maxInterval = 30 * time.Second
	}
	if batchSize <= 0 {
		batchSize = 50
	}
	return &Relay{store: store, client: client, baseInterval: baseInterval, maxInterval: maxInterval, batchSize: batchSize}
}

// Run polls until ctx is done.
func (r *Relay) Run(ctx context.Context) error {
	interval := r.baseInterval
	timer := time.NewTimer(interval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
		}

		claimed, dispatched, failed, err := r.store.DispatchBatch(ctx, r.batchSize, r.dispatchOne)
		if err != nil {
			slog.Error("outbox relay: dispatch batch", "error", err)
			interval = r.backoff(interval)
		} else if failed > 0 {
			slog.Warn("outbox relay: some rows failed to dispatch, will retry", "claimed", claimed, "dispatched", dispatched, "failed", failed)
			interval = r.backoff(interval)
		} else {
			if claimed > 0 {
				slog.Debug("outbox relay: dispatched batch", "claimed", claimed, "dispatched", dispatched)
			}
			interval = r.baseInterval
		}

		timer.Reset(interval)
	}
}

func (r *Relay) backoff(current time.Duration) time.Duration {
	next := current * 2
	if next > r.maxInterval {
		next = r.maxInterval
	}
	if next < r.baseInterval {
		next = r.baseInterval
	}
	// Jitter: +/- up to 25% of next, so a fleet of relay instances (or a
	// restarted single one) don't all re-poll in lockstep.
	jitterRange := int64(next) / 4
	if jitterRange <= 0 {
		return next
	}
	delta := rand.Int64N(2*jitterRange) - jitterRange
	return next + time.Duration(delta)
}

// dispatchOne is the per-row dispatch callback DispatchBatch invokes,
// deciding EnqueueOptions from the row's task type: TaskTypeCharge gets
// MaxRetry(0) (plan: "charge task no-auto-retry, task lain retry normal"),
// everything else keeps the queue's default retry behavior. It never calls
// a provider -- see the invariant documented in store.go's package doc and
// in payment.Service.ProcessCharge.
func (r *Relay) dispatchOne(ctx context.Context, row Row) error {
	opts := queue.EnqueueOptions{TaskID: row.ID.String()}
	if row.TaskType == queue.TaskTypeCharge {
		zero := 0
		opts.MaxRetry = &zero
	}
	return r.client.Enqueue(ctx, row.TaskType, row.Payload, opts)
}
