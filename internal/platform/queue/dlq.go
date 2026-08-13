package queue

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TerminalFailureRecorder writes to terminal_failures, the DLQ inspection
// table (plan: "asynq archive queue (built-in) + tabel terminal_failures
// di-populate via error handler callback asynq"). It is wired into two
// places: asynq.Config.ErrorHandler (real Redis path, fires when a task
// exhausts retries and asynq archives it) and MemoryClient's ErrorRecorder
// (in-process fallback, which has no asynq server/archive concept at all).
//
// This must only ever fire for genuinely unexpected failures (unregistered
// provider, malformed payload, a DB write failing) -- never for the
// expected "charge task attempted the provider call once, it timed out,
// payment stays processing" outcome. That outcome is reported by returning
// nil from the charge handler (see payment.Service.ProcessCharge), so it
// never reaches here or asynq's archive in the first place.
type TerminalFailureRecorder struct {
	pool *pgxpool.Pool
}

func NewTerminalFailureRecorder(pool *pgxpool.Pool) *TerminalFailureRecorder {
	return &TerminalFailureRecorder{pool: pool}
}

// Record inserts one terminal_failures row. Failures to record are logged,
// not propagated -- a DLQ bookkeeping problem must never cascade into the
// task-processing path.
func (r *TerminalFailureRecorder) Record(ctx context.Context, taskType, taskID string, payload []byte, errMsg string) {
	const insertSQL = `
		INSERT INTO terminal_failures (task_type, task_id, payload, error)
		VALUES ($1, $2, $3, $4)`
	if _, err := r.pool.Exec(ctx, insertSQL, taskType, taskID, payload, errMsg); err != nil {
		slog.Error("queue: record terminal failure", "error", err, "task_type", taskType, "task_id", taskID)
	}
}

// Count reports the cumulative number of terminal_failures rows -- the
// Fase 5 DLQ-depth metric's data source (internal/platform/observability.Poller
// polls this periodically). terminal_failures has no resolution/ack column,
// so this is a monotonically non-decreasing total, not a live queue depth --
// see docs/observability-alerts.md for how the alert threshold accounts for
// that.
func (r *TerminalFailureRecorder) Count(ctx context.Context) (int64, error) {
	const sql = `SELECT count(*) FROM terminal_failures`
	var n int64
	if err := r.pool.QueryRow(ctx, sql).Scan(&n); err != nil {
		return 0, fmt.Errorf("queue: count terminal failures: %w", err)
	}
	return n, nil
}

// AsynqErrorHandler adapts Record into an asynq.ErrorHandlerFunc for
// asynq.Config.ErrorHandler. Only records once a task has exhausted its
// retries (retried >= maxRetry) -- earlier failures are still going to be
// retried by asynq itself and are not yet "terminal".
func (r *TerminalFailureRecorder) AsynqErrorHandler() asynq.ErrorHandlerFunc {
	return func(ctx context.Context, task *asynq.Task, err error) {
		retried, _ := asynq.GetRetryCount(ctx)
		maxRetry, _ := asynq.GetMaxRetry(ctx)
		if retried < maxRetry {
			return
		}
		taskID, _ := asynq.GetTaskID(ctx)
		r.Record(ctx, task.Type(), taskID, task.Payload(), err.Error())
	}
}

// MemoryErrorRecorder adapts Record into the ErrorRecorder shape
// MemoryClient expects -- every handler error in memory mode is immediately
// terminal (there is no retry/archive concept there at all).
func (r *TerminalFailureRecorder) MemoryErrorRecorder() ErrorRecorder {
	return func(ctx context.Context, taskType, taskID string, payload []byte, err error) {
		r.Record(ctx, taskType, taskID, payload, err.Error())
	}
}
