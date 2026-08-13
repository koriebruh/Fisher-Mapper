package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"Fisher-Mapper/internal/platform/queue"
)

// providerCarryingTaskTypes are the outbox task types whose payload has a
// "provider" JSON field the Fase 4 provider-enabled check can read -- both
// charge and refund tasks (queue.TaskTypeCharge / queue.TaskTypeRefund)
// carry one (see payment.ChargeTaskInput / payment.RefundTaskInput). Kept
// as a set here, rather than importing package payment for the struct
// shape, to avoid an outbox<->payment import cycle (payment already
// imports outbox to build these very rows).
var providerCarryingTaskTypes = map[string]bool{
	queue.TaskTypeCharge: true,
	queue.TaskTypeRefund: true,
}

// providerCarrier is the minimal shape shared by ChargeTaskInput and
// RefundTaskInput needed to extract the provider name from a raw payload
// without importing package payment.
type providerCarrier struct {
	Provider string `json:"provider"`
}

// Relay periodically drains pending outbox rows to a queue.Client. It is
// the actor internal/platform/lifecycle.RunnerActor wraps for oklog/run.Group.
type Relay struct {
	store        *Store
	client       queue.Client
	baseInterval time.Duration
	maxInterval  time.Duration
	batchSize    int

	// providerEnabled is the Fase 4 dynamic-config check (plan: "worker
	// WAJIB cek ulang flag provider-enabled ... bukan cuma dicek sekali pas
	// enqueue" -- this is the enqueue-time half of that requirement, the
	// other half being payment.Service.ProcessCharge/ProcessRefund's own
	// check right before the provider call). nil means "always enabled".
	providerEnabled func(providerName string) bool

	// queueName is called once per dispatch to get the asynq queue to
	// enqueue into. nil or "" means asynq's implicit default queue. Wired
	// as a func (not a plain string) so the relay can pick up a config
	// change without restart, exactly like providerEnabled -- but see
	// cmd/worker/main.go: it deliberately snapshots this to a fixed value
	// once at boot instead, because the consuming asynq.Server's queue set
	// is fixed at construction and cannot be changed live.
	queueName func() string
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

// WithProviderEnabledCheck wires the Fase 4 provider-enabled cache check
// into dispatchOne. Returns r for chaining.
func (r *Relay) WithProviderEnabledCheck(fn func(providerName string) bool) *Relay {
	r.providerEnabled = fn
	return r
}

// WithQueueName wires the queue-name getter into dispatchOne. Returns r for
// chaining.
func (r *Relay) WithQueueName(fn func() string) *Relay {
	r.queueName = fn
	return r
}

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
// deciding EnqueueOptions from the row's task type: TaskTypeCharge and
// TaskTypeRefund both get MaxRetry(0) (plan: "charge task no-auto-retry,
// task lain retry normal" -- refund carries the identical double-provider-
// call risk, per the Fase 4 task instructions, so it gets the same
// treatment), everything else keeps the queue's default retry behavior. It
// never calls a provider -- see the invariant documented in store.go's
// package doc and in payment.Service.ProcessCharge/ProcessRefund.
//
// Fase 4 addition: for charge/refund rows, checks the dynamic-config
// provider-enabled flag BEFORE enqueueing. Returning an error here (rather
// than silently dropping the row) is exactly what DispatchBatch already
// does for a failed dispatch attempt -- the row stays 'pending', attempts
// increments, last_error records why, and the relay's own retry-with-
// backoff loop naturally re-attempts it on a later tick, which is also
// exactly what makes "flip the flag back on" self-heal without any extra
// mechanism.
func (r *Relay) dispatchOne(ctx context.Context, row Row) error {
	if r.providerEnabled != nil && providerCarryingTaskTypes[row.TaskType] {
		var carrier providerCarrier
		if err := json.Unmarshal(row.Payload, &carrier); err != nil {
			return fmt.Errorf("outbox: dispatch: decode provider from payload: %w", err)
		}
		if carrier.Provider != "" && !r.providerEnabled(carrier.Provider) {
			return fmt.Errorf("outbox: dispatch: provider %q is disabled via dynamic config", carrier.Provider)
		}
	}

	opts := queue.EnqueueOptions{TaskID: row.ID.String()}
	if row.TaskType == queue.TaskTypeCharge || row.TaskType == queue.TaskTypeRefund {
		zero := 0
		opts.MaxRetry = &zero
	}
	if r.queueName != nil {
		opts.QueueName = r.queueName()
	}
	return r.client.Enqueue(ctx, row.TaskType, row.Payload, opts)
}
