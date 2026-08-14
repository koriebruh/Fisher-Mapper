package payment

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"Fisher-Mapper/internal/domain/apperror"
)

// TransitionParams bundles the arguments for Repository.ApplyTransition.
type TransitionParams struct {
	PaymentID uuid.UUID
	To        Status
	EventTS   time.Time
	EventType string
	Provider  string

	// ProviderRef, if non-nil, is written to payments.provider_ref as part
	// of the same update (e.g. the reference the provider assigned on the
	// call that drove this transition).
	ProviderRef *string

	// ProviderEventID, if non-nil, is the provider's own event id — the
	// dedup key enforced by the unique index on
	// payment_events(provider, provider_event_id). Internally-driven
	// transitions (e.g. pending -> processing right after we create the
	// row) leave this nil since there is no provider event backing them.
	ProviderEventID *string

	RawPayload []byte

	// InitiatedBy is the per-TRANSITION actor (see Envelope.InitiatedBy's
	// doc for the aggregate-level analogue): "system" for every worker/
	// webhook/reconciliation-driven transition today, "admin" reserved for
	// a future manual-retry endpoint. Every call site must set this
	// explicitly (the migration drops the column's DEFAULT after backfill)
	// -- callers should use InitiatedBySystem unless they are that future
	// admin path.
	InitiatedBy string
}

// InitiatedBySystem/InitiatedByCustomer/InitiatedByAdmin are the only valid
// InitiatedBy values (CHECK constraint on payment_events/refund_events/
// payout_events.initiated_by) -- defined as constants so call sites never
// hand-type the string.
const (
	InitiatedByCustomer = "customer"
	InitiatedBySystem   = "system"
	InitiatedByAdmin    = "admin"
)

// Repository is the payment persistence interface. The pgx implementation
// (PGRepository) is the only one that ships with Phase 2 — sqlc generation
// was deliberately deferred (see plan report); every query here is
// transaction-scoped control flow (BEGIN -> SELECT ... FOR UPDATE -> branch
// in Go -> UPDATE + INSERT -> COMMIT) that sqlc does not generate for you
// regardless.
type Repository interface {
	// Create inserts a new payment row in StatusPending and populates
	// p.ID/CreatedAt/UpdatedAt/LastEventAt from the DB defaults.
	Create(ctx context.Context, p *Payment) error

	Get(ctx context.Context, id uuid.UUID) (*Payment, error)

	// FindByProviderRef looks up a payment by (provider, providerRef),
	// used when applying a provider webhook event.
	FindByProviderRef(ctx context.Context, provider, providerRef string) (*Payment, error)

	// SetProviderRef persists provider_ref without changing status — used
	// when a provider call returns a reference but the payment stays in
	// its current state (e.g. still "processing"), so a full
	// ApplyTransition (which would reject a same-status "transition")
	// isn't the right tool.
	SetProviderRef(ctx context.Context, id uuid.UUID, providerRef string) error

	// ApplyTransition performs, in one transaction:
	//  1. dedup check (if ProviderEventID is set): if an event with the
	//     same (provider, provider_event_id) was already applied, return
	//     apperror.CodeDuplicateEvent and change nothing.
	//  2. SELECT status, last_event_at FROM payments WHERE id = $1 FOR
	//     UPDATE (row lock).
	//  3. Transition(current, params.To, params.EventTS, lastEventAt) —
	//     returns apperror.CodeTerminalState / CodeStaleEvent /
	//     CodeInvalidTransition without writing anything if rejected.
	//  4. UPDATE payments (status, last_event_at, provider_ref if set).
	//  5. INSERT INTO payment_events.
	//  6. COMMIT.
	//
	// Called both by the Fase 2 webhook-apply path AND, as of Fase 3, by
	// the worker's pending->processing compare-and-swap
	// (Service.ProcessCharge) — that CAS, not anything in the queue layer,
	// is what guarantees a redelivered/duplicate charge task never calls
	// the provider twice: a second delivery's pending->processing attempt
	// finds the row already "processing" (or terminal) and is rejected by
	// Transition, exactly like any other invalid transition.
	ApplyTransition(ctx context.Context, params TransitionParams) error

	// CreateWithOutbox inserts a new payment row in StatusPending and, in
	// the SAME transaction, invokes withTx (given the open pgx.Tx) so a
	// caller can persist an outbox row (or any other side effect) that must
	// never exist without the payment existing, and vice versa — the Fase 3
	// addendum's atomicity requirement for the async create-payment path.
	// withTx returning an error rolls back the whole transaction, including
	// the payment insert.
	CreateWithOutbox(ctx context.Context, p *Payment, withTx func(ctx context.Context, tx pgx.Tx) error) error

	// ListProcessingOlderThan returns payments in StatusProcessing whose
	// last_event_at is older than cutoff — the Fase 4 reconciliation job's
	// "poll processing stuck past some threshold" query (plan Fase 4).
	ListProcessingOlderThan(ctx context.Context, cutoff time.Time) ([]*Payment, error)
}

// PGRepository is the pgx-backed Repository implementation.
type PGRepository struct {
	pool *pgxpool.Pool
}

func NewPGRepository(pool *pgxpool.Pool) *PGRepository {
	return &PGRepository{pool: pool}
}

const paymentInsertColumns = `tenant_id, livemode, currency, amount, operation_type, provider, status,
		    source_app, channel, trace_id, description, initiated_by, request_ip, request_user_agent`
const paymentInsertPlaceholders = `$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14`

func (r *PGRepository) Create(ctx context.Context, p *Payment) error {
	insertSQL := `INSERT INTO payments (` + paymentInsertColumns + `)
		VALUES (` + paymentInsertPlaceholders + `)
		RETURNING id, status, last_event_at, created_at, updated_at`

	err := r.pool.QueryRow(ctx, insertSQL,
		p.TenantID, p.Livemode, p.Currency, p.Amount, p.OperationType, p.Provider, StatusPending,
		p.SourceApp, p.Channel, p.TraceID, p.Description, p.InitiatedBy, p.RequestIP, p.RequestUserAgent,
	).Scan(&p.ID, &p.Status, &p.LastEventAt, &p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		return fmt.Errorf("payment: create: %w", err)
	}
	return nil
}

func (r *PGRepository) CreateWithOutbox(ctx context.Context, p *Payment, withTx func(ctx context.Context, tx pgx.Tx) error) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("payment: create with outbox: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	insertSQL := `INSERT INTO payments (` + paymentInsertColumns + `)
		VALUES (` + paymentInsertPlaceholders + `)
		RETURNING id, status, last_event_at, created_at, updated_at`

	err = tx.QueryRow(ctx, insertSQL,
		p.TenantID, p.Livemode, p.Currency, p.Amount, p.OperationType, p.Provider, StatusPending,
		p.SourceApp, p.Channel, p.TraceID, p.Description, p.InitiatedBy, p.RequestIP, p.RequestUserAgent,
	).Scan(&p.ID, &p.Status, &p.LastEventAt, &p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		return fmt.Errorf("payment: create with outbox: insert payment: %w", err)
	}

	if err := withTx(ctx, tx); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("payment: create with outbox: commit: %w", err)
	}
	return nil
}

func (r *PGRepository) Get(ctx context.Context, id uuid.UUID) (*Payment, error) {
	const selectSQL = `
		SELECT id, tenant_id, livemode, currency, amount, operation_type, provider,
		       provider_ref, status, last_event_at, created_at, updated_at
		FROM payments WHERE id = $1`

	p := &Payment{}
	err := r.pool.QueryRow(ctx, selectSQL, id).Scan(
		&p.ID, &p.TenantID, &p.Livemode, &p.Currency, &p.Amount, &p.OperationType, &p.Provider,
		&p.ProviderRef, &p.Status, &p.LastEventAt, &p.CreatedAt, &p.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, apperror.New(apperror.CodeNotFound, "payment: not found")
		}
		return nil, fmt.Errorf("payment: get: %w", err)
	}
	return p, nil
}

func (r *PGRepository) FindByProviderRef(ctx context.Context, provider, providerRef string) (*Payment, error) {
	const selectSQL = `
		SELECT id, tenant_id, livemode, currency, amount, operation_type, provider,
		       provider_ref, status, last_event_at, created_at, updated_at
		FROM payments WHERE provider = $1 AND provider_ref = $2`

	p := &Payment{}
	err := r.pool.QueryRow(ctx, selectSQL, provider, providerRef).Scan(
		&p.ID, &p.TenantID, &p.Livemode, &p.Currency, &p.Amount, &p.OperationType, &p.Provider,
		&p.ProviderRef, &p.Status, &p.LastEventAt, &p.CreatedAt, &p.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, apperror.New(apperror.CodeNotFound, "payment: not found for provider ref")
		}
		return nil, fmt.Errorf("payment: find by provider ref: %w", err)
	}
	return p, nil
}

func (r *PGRepository) ApplyTransition(ctx context.Context, params TransitionParams) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("payment: apply transition: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	if params.ProviderEventID != nil {
		var exists bool
		const dedupSQL = `SELECT EXISTS(SELECT 1 FROM payment_events WHERE provider = $1 AND provider_event_id = $2)`
		if err := tx.QueryRow(ctx, dedupSQL, params.Provider, *params.ProviderEventID).Scan(&exists); err != nil {
			return fmt.Errorf("payment: apply transition: dedup check: %w", err)
		}
		if exists {
			return apperror.New(apperror.CodeDuplicateEvent, "payment: provider_event_id already applied")
		}
	}

	const lockSQL = `SELECT status, last_event_at FROM payments WHERE id = $1 FOR UPDATE`
	var current Status
	var lastEventAt time.Time
	if err := tx.QueryRow(ctx, lockSQL, params.PaymentID).Scan(&current, &lastEventAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return apperror.New(apperror.CodeNotFound, "payment: not found")
		}
		return fmt.Errorf("payment: apply transition: lock: %w", err)
	}

	if err := Transition(current, params.To, params.EventTS, lastEventAt); err != nil {
		return err
	}

	const updateSQL = `
		UPDATE payments
		SET status = $1, last_event_at = $2, updated_at = now(),
		    provider_ref = coalesce($3, provider_ref)
		WHERE id = $4`
	if _, err := tx.Exec(ctx, updateSQL, params.To, params.EventTS, params.ProviderRef, params.PaymentID); err != nil {
		return fmt.Errorf("payment: apply transition: update: %w", err)
	}

	const insertEventSQL = `
		INSERT INTO payment_events (payment_id, event_type, provider, provider_event_id, provider_event_ts, raw_payload)
		VALUES ($1, $2, $3, $4, $5, $6)`
	if _, err := tx.Exec(ctx, insertEventSQL,
		params.PaymentID, params.EventType, params.Provider, params.ProviderEventID, params.EventTS, params.RawPayload,
	); err != nil {
		return fmt.Errorf("payment: apply transition: insert event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("payment: apply transition: commit: %w", err)
	}
	return nil
}

func (r *PGRepository) SetProviderRef(ctx context.Context, id uuid.UUID, providerRef string) error {
	const updateSQL = `UPDATE payments SET provider_ref = $1, updated_at = now() WHERE id = $2`
	tag, err := r.pool.Exec(ctx, updateSQL, providerRef, id)
	if err != nil {
		return fmt.Errorf("payment: set provider ref: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return apperror.New(apperror.CodeNotFound, "payment: not found")
	}
	return nil
}

func (r *PGRepository) ListProcessingOlderThan(ctx context.Context, cutoff time.Time) ([]*Payment, error) {
	const selectSQL = `
		SELECT id, tenant_id, livemode, currency, amount, operation_type, provider,
		       provider_ref, status, last_event_at, created_at, updated_at
		FROM payments
		WHERE status = 'processing' AND last_event_at < $1
		ORDER BY last_event_at`

	rows, err := r.pool.Query(ctx, selectSQL, cutoff)
	if err != nil {
		return nil, fmt.Errorf("payment: list processing older than: %w", err)
	}
	defer rows.Close()

	var out []*Payment
	for rows.Next() {
		p := &Payment{}
		if err := rows.Scan(
			&p.ID, &p.TenantID, &p.Livemode, &p.Currency, &p.Amount, &p.OperationType, &p.Provider,
			&p.ProviderRef, &p.Status, &p.LastEventAt, &p.CreatedAt, &p.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("payment: list processing older than: scan: %w", err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("payment: list processing older than: %w", err)
	}
	return out, nil
}

// Seen implements auth.DedupChecker so an HMACVerifier can be wired
// directly against the payment_events table without the auth package
// importing this one (it takes the interface, we satisfy it structurally).
func (r *PGRepository) Seen(ctx context.Context, provider, providerEventID string) (bool, error) {
	if providerEventID == "" {
		return false, nil
	}
	var exists bool
	const sql = `SELECT EXISTS(SELECT 1 FROM payment_events WHERE provider = $1 AND provider_event_id = $2)`
	if err := r.pool.QueryRow(ctx, sql, provider, providerEventID).Scan(&exists); err != nil {
		return false, fmt.Errorf("payment: seen: %w", err)
	}
	return exists, nil
}

var _ Repository = (*PGRepository)(nil)
