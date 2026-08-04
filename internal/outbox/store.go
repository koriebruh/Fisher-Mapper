// Package outbox implements the transactional-outbox pattern backing async
// charge dispatch (Fase 3 addendum): a domain write (payment row insert) and
// an outbox row are inserted in one Postgres transaction, so "the payment
// exists" and "a task exists to process it" can never diverge. A separate
// Relay polls this table and hands rows to a queue.Client.
//
// Design invariant (plan "Catatan desain wajib"): retrying the *dispatch* of
// an outbox row is safe -- re-enqueueing is idempotent by construction (see
// Relay's deterministic TaskID). Retrying the *provider call* the resulting
// task triggers is NOT safe, and this package has no opinion on that at
// all -- that guarantee lives entirely in the payment state machine's
// pending->processing compare-and-swap (see payment.Service.ProcessCharge),
// one layer up. Nothing here re-attempts a charge; it only ever moves bytes
// from Postgres to a queue.
package outbox

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Row is a claimed outbox row.
type Row struct {
	ID       uuid.UUID
	TaskType string
	Payload  []byte
	Attempts int
}

// queryRower is satisfied by both *pgxpool.Pool and pgx.Tx -- Insert works
// against either, so a caller building a transaction that also writes a
// domain row (see payment.Repository.CreateWithOutbox) can pass its own
// pgx.Tx straight through.
type queryRower interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Insert writes a new pending outbox row inside whatever transaction q
// represents (or, if q is a pool, as its own implicit transaction).
func Insert(ctx context.Context, q queryRower, taskType string, payload []byte) (uuid.UUID, error) {
	const insertSQL = `
		INSERT INTO outbox (task_type, payload, status)
		VALUES ($1, $2, 'pending')
		RETURNING id`

	var id uuid.UUID
	if err := q.QueryRow(ctx, insertSQL, taskType, payload).Scan(&id); err != nil {
		return uuid.Nil, fmt.Errorf("outbox: insert: %w", err)
	}
	return id, nil
}

// Store owns the relay-side polling/claim/dispatch-marking logic.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore builds a Store over an existing pool.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// claim runs inside tx: SELECT ... FOR UPDATE SKIP LOCKED up to limit
// pending rows. Exported at package level (lowercase, tested via a
// same-package test) so the SKIP LOCKED guarantee -- two concurrent callers
// never claim the same row -- can be exercised directly against two
// independently-held transactions, which DispatchBatch's own
// begin-claim-dispatch-commit loop doesn't let a test observe from outside.
func claim(ctx context.Context, tx pgx.Tx, limit int) ([]Row, error) {
	const selectSQL = `
		SELECT id, task_type, payload, attempts
		FROM outbox
		WHERE status = 'pending'
		ORDER BY created_at
		FOR UPDATE SKIP LOCKED
		LIMIT $1`

	rows, err := tx.Query(ctx, selectSQL, limit)
	if err != nil {
		return nil, fmt.Errorf("outbox: claim: %w", err)
	}
	defer rows.Close()

	var out []Row
	for rows.Next() {
		var r Row
		if err := rows.Scan(&r.ID, &r.TaskType, &r.Payload, &r.Attempts); err != nil {
			return nil, fmt.Errorf("outbox: claim: scan: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("outbox: claim: %w", err)
	}
	return out, nil
}

// DispatchBatch claims up to limit pending rows and calls dispatch for each,
// all inside one transaction -- claimed rows stay row-locked for the
// duration, so a concurrent DispatchBatch call (another relay instance, or
// this same process's next tick if ever run concurrently) never sees them.
//
// dispatch succeeding marks the row 'dispatched'. dispatch failing (e.g.
// Redis unreachable) leaves the row 'pending' with attempts incremented and
// last_error recorded, for the next call to retry -- exactly the "retry
// dispatch is safe" half of the design invariant; dispatch itself must never
// call a provider (see package doc).
func (s *Store) DispatchBatch(ctx context.Context, limit int, dispatch func(ctx context.Context, row Row) error) (claimed, dispatched, failed int, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("outbox: dispatch batch: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	rows, err := claim(ctx, tx, limit)
	if err != nil {
		return 0, 0, 0, err
	}
	claimed = len(rows)

	for _, row := range rows {
		if derr := dispatch(ctx, row); derr != nil {
			const updSQL = `UPDATE outbox SET attempts = attempts + 1, last_error = $1 WHERE id = $2`
			if _, uerr := tx.Exec(ctx, updSQL, derr.Error(), row.ID); uerr != nil {
				return claimed, dispatched, failed, fmt.Errorf("outbox: record dispatch failure: %w", uerr)
			}
			failed++
			continue
		}

		const updSQL = `UPDATE outbox SET status = 'dispatched', dispatched_at = now() WHERE id = $1`
		if _, uerr := tx.Exec(ctx, updSQL, row.ID); uerr != nil {
			return claimed, dispatched, failed, fmt.Errorf("outbox: mark dispatched: %w", uerr)
		}
		dispatched++
	}

	if err := tx.Commit(ctx); err != nil {
		return claimed, dispatched, failed, fmt.Errorf("outbox: dispatch batch: commit: %w", err)
	}
	return claimed, dispatched, failed, nil
}

// PendingCount reports how many rows are still 'pending' -- used by tests
// and could back a future outbox-lag metric (Fase 5).
func (s *Store) PendingCount(ctx context.Context) (int, error) {
	const sql = `SELECT count(*) FROM outbox WHERE status = 'pending'`
	var n int
	if err := s.pool.QueryRow(ctx, sql).Scan(&n); err != nil {
		return 0, fmt.Errorf("outbox: pending count: %w", err)
	}
	return n, nil
}
