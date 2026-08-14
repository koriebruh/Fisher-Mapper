// Package reconciliation is a thin oklog/run actor: a ticker loop that
// periodically calls into payment.Service's reconciliation methods (which
// own every piece of actual business logic -- the amount/currency
// verification, the state-machine application, the staged-webhook joins).
// This package intentionally contains no money-invariant logic of its own;
// see internal/domain/payment/reconcile.go for that.
package reconciliation

import (
	"context"
	"log/slog"
	"time"

	"Fisher-Mapper/internal/domain/payment"
)

// service is the subset of *payment.Service Job depends on, declared as an
// interface (same pattern as config.configSource / payment's own
// providerRegistry) so the reconciliation.enabled toggle's behavior can be
// unit tested against a fake, without a live Postgres connection.
// *payment.Service satisfies this structurally.
type service interface {
	ListStuckProcessing(ctx context.Context, threshold time.Duration) ([]*payment.Payment, error)
	ReconcilePayment(ctx context.Context, p *payment.Payment) error
	SweepStagedWebhooks(ctx context.Context) (int, error)
}

// Job periodically:
//  1. polls payments stuck in "processing" past Threshold and resolves each
//     via Service.ReconcilePayment (join staged webhooks, then verify
//     amount+currency via GetStatus before applying any transition);
//  2. sweeps staged webhook events that still have no matching payment via
//     Service.SweepStagedWebhooks (the gap Fase 3's report explicitly left
//     to Fase 4).
type Job struct {
	service   service
	interval  time.Duration
	threshold time.Duration

	// enabled is the dynamic-config reconciliation.enabled check. nil (or a
	// func returning true) means "run as normal"; false pauses the job --
	// e.g. during a maintenance window -- without stopping the rest of the
	// worker process (the oklog/run actor keeps ticking, it just skips the
	// work each tick).
	enabled func() bool
}

// New builds a Job. interval is how often the poll runs; threshold is how
// long a payment must have been sitting in "processing" (since its last
// applied event) before this job will touch it at all -- deliberately NOT
// the same value as interval, since a payment that entered "processing" a
// second ago almost certainly just has its ProcessCharge call still
// in-flight, not stuck.
func New(svc *payment.Service, interval, threshold time.Duration) *Job {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	if threshold <= 0 {
		threshold = 2 * time.Minute
	}
	return &Job{service: svc, interval: interval, threshold: threshold}
}

// WithEnabledCheck wires the dynamic-config reconciliation.enabled flag
// check (see enabled's doc). Returns j for chaining.
func (j *Job) WithEnabledCheck(fn func() bool) *Job {
	j.enabled = fn
	return j
}

// Run loops until ctx is done -- the actor internal/platform/lifecycle.RunnerActor
// wraps for oklog/run.Group.
func (j *Job) Run(ctx context.Context) error {
	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			j.runOnce(ctx)
		}
	}
}

// RunOnce performs a single reconciliation pass immediately -- exported so
// tests (and, for the docker-compose live validation, an operator wanting
// to force an immediate pass rather than wait for the ticker) don't have to
// wait out interval.
func (j *Job) RunOnce(ctx context.Context) {
	j.runOnce(ctx)
}

func (j *Job) runOnce(ctx context.Context) {
	if j.enabled != nil && !j.enabled() {
		slog.Debug("reconciliation: pass skipped, disabled via dynamic config")
		return
	}

	stuck, err := j.service.ListStuckProcessing(ctx, j.threshold)
	if err != nil {
		slog.Error("reconciliation: list stuck processing", "error", err)
	} else {
		for _, p := range stuck {
			if err := j.service.ReconcilePayment(ctx, p); err != nil {
				slog.Error("reconciliation: resolve payment", "error", err, "payment_id", p.ID)
			}
		}
		if len(stuck) > 0 {
			slog.Info("reconciliation: pass complete", "stuck_payments_seen", len(stuck))
		}
	}

	if matched, err := j.service.SweepStagedWebhooks(ctx); err != nil {
		slog.Error("reconciliation: sweep staged webhooks", "error", err)
	} else if matched > 0 {
		slog.Info("reconciliation: staged webhook sweep matched payments", "count", matched)
	}
}
