package observability

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Poller is the Fase 5 background actor for metrics that only make sense as
// periodic snapshots (DB pool stats, DLQ/terminal-failures depth) rather
// than recorded inline at some call site -- wired into oklog/run.Group by
// internal/platform/lifecycle.RunnerActor, same as the outbox relay and the
// dynamic-config cache refresh loop.
type Poller struct {
	pool     *pgxpool.Pool
	dlqCount func(ctx context.Context) (int64, error)
	metrics  *Metrics
	interval time.Duration
}

// NewPoller builds a Poller. pool may be nil (skip DB pool gauges); dlqCount
// may be nil (skip the terminal_failures gauge -- e.g. cmd/server, which
// never writes to that table, doesn't bother polling it, only cmd/worker
// does).
func NewPoller(pool *pgxpool.Pool, dlqCount func(ctx context.Context) (int64, error), metrics *Metrics, interval time.Duration) *Poller {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	return &Poller{pool: pool, dlqCount: dlqCount, metrics: metrics, interval: interval}
}

// Run loops until ctx is done.
func (p *Poller) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	// Poll once immediately so the first Prometheus scrape after startup
	// already has a value rather than waiting a full interval.
	p.pollOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			p.pollOnce(ctx)
		}
	}
}

func (p *Poller) pollOnce(ctx context.Context) {
	if p.metrics == nil {
		return
	}

	if p.pool != nil {
		stat := p.pool.Stat()
		p.metrics.DBPoolAcquiredConns.Record(ctx, int64(stat.AcquiredConns()))
		p.metrics.DBPoolIdleConns.Record(ctx, int64(stat.IdleConns()))
		p.metrics.DBPoolTotalConns.Record(ctx, int64(stat.TotalConns()))
	}

	if p.dlqCount != nil {
		n, err := p.dlqCount(ctx)
		if err != nil {
			slog.Warn("[observability] pollOnce: dlq count failed, keeping last value", "component", "observability", "error", err)
			return
		}
		p.metrics.TerminalFailuresTotal.Record(ctx, n)
	}
}
