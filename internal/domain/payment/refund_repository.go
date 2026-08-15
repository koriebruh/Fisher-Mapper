package payment

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"Fisher-Mapper/internal/domain/apperror"
)

// RefundTransitionParams bundles the arguments for
// Repository.ApplyRefundTransition — the refund-table analogue of
// TransitionParams.
type RefundTransitionParams struct {
	RefundID  uuid.UUID
	To        Status
	EventTS   time.Time
	EventType string
	Provider  string

	// ProviderRefundRef, if non-nil, is written to refunds.provider_refund_ref
	// as part of the same update (the reference the provider assigned to
	// this refund on the call that drove this transition).
	ProviderRefundRef *string

	ProviderEventID *string
	RawPayload      []byte

	// InitiatedBy is the per-TRANSITION actor -- see TransitionParams.InitiatedBy's
	// doc for the full taxonomy. Every call site must set this explicitly.
	InitiatedBy string
}

// CreateRefundWithOutbox validates and inserts a new refund row, in ONE
// Postgres transaction that also:
//  1. SELECT ... FOR UPDATE locks the parent payment row — this is what
//     serializes concurrent refund-create attempts against the SAME
//     payment, which the sum(refunds)<=original check below depends on
//     (plan Decide Now item 10: "minimal application-level lock + validasi
//     sebelum insert dalam transaksi yang sama").
//  2. Rejects if the payment is not "succeeded" (only a completed charge can
//     be refunded).
//  3. Sums existing refunds in pending/processing/succeeded (NOT failed —
//     a failed refund never happened) against this payment and rejects with
//     apperror.CodeRefundLimitExceeded if adding r.Amount would exceed the
//     original payment's amount.
//  4. Copies provider/provider_ref from the locked payment onto r (a refund
//     always targets the same provider and provider_ref as its parent
//     charge; the caller does not supply these).
//  5. Inserts the refund row (status pending).
//  6. Invokes withTx (e.g. to insert an outbox row) inside the SAME
//     transaction, exactly like Repository.CreateWithOutbox.
func (r *PGRepository) CreateRefundWithOutbox(ctx context.Context, ref *Refund, withTx func(ctx context.Context, tx pgx.Tx) error) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("refund: create with outbox: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	const lockPaymentSQL = `
		SELECT amount, currency, provider, provider_ref, status
		FROM payments WHERE id = $1 FOR UPDATE`
	var paymentAmount int64
	var paymentCurrency, paymentProvider string
	var paymentProviderRef *string
	var paymentStatus Status
	if err := tx.QueryRow(ctx, lockPaymentSQL, ref.PaymentID).Scan(
		&paymentAmount, &paymentCurrency, &paymentProvider, &paymentProviderRef, &paymentStatus,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return apperror.New(apperror.CodeNotFound, "refund: payment not found")
		}
		return fmt.Errorf("refund: create with outbox: lock payment: %w", err)
	}

	if paymentStatus != StatusSucceeded {
		return apperror.New(apperror.CodeInvalidTransition, "refund: payment is not in a succeeded state, cannot refund")
	}
	if ref.Currency != "" && ref.Currency != paymentCurrency {
		return apperror.New(apperror.CodeValidation, "refund: currency must match the original payment's currency")
	}

	const sumSQL = `
		SELECT coalesce(sum(amount), 0) FROM refunds
		WHERE payment_id = $1 AND status IN ('pending', 'processing', 'succeeded')`
	var alreadyRefunded int64
	if err := tx.QueryRow(ctx, sumSQL, ref.PaymentID).Scan(&alreadyRefunded); err != nil {
		return fmt.Errorf("refund: create with outbox: sum existing refunds: %w", err)
	}
	if alreadyRefunded+ref.Amount > paymentAmount {
		return apperror.New(apperror.CodeRefundLimitExceeded, fmt.Sprintf(
			"refund: amount %d plus already-refunded %d would exceed original payment amount %d",
			ref.Amount, alreadyRefunded, paymentAmount))
	}

	ref.Currency = paymentCurrency
	ref.Provider = paymentProvider
	ref.ProviderRef = paymentProviderRef

	const insertSQL = `
		INSERT INTO refunds (payment_id, tenant_id, livemode, currency, amount, operation_type, provider, provider_ref, status,
		    source_app, channel, trace_id, description, initiated_by, request_ip, request_user_agent)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
		RETURNING id, status, last_event_at, created_at, updated_at`
	err = tx.QueryRow(ctx, insertSQL,
		ref.PaymentID, ref.TenantID, ref.Livemode, ref.Currency, ref.Amount, OperationRefund, ref.Provider, ref.ProviderRef, StatusPending,
		ref.SourceApp, ref.Channel, ref.TraceID, ref.Description, ref.InitiatedBy, ref.RequestIP, ref.RequestUserAgent,
	).Scan(&ref.ID, &ref.Status, &ref.LastEventAt, &ref.CreatedAt, &ref.UpdatedAt)
	if err != nil {
		return fmt.Errorf("refund: create with outbox: insert refund: %w", err)
	}

	if err := withTx(ctx, tx); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("refund: create with outbox: commit: %w", err)
	}
	return nil
}

// ApplyRefundTransition is the refund-table analogue of ApplyTransition:
// same dedup / row-lock / Transition / update / append-only-event / commit
// shape, applied to refunds + refund_events instead of payments +
// payment_events. This is what gives the refund flow the identical
// redelivery/double-refund safety ProcessCharge's CAS gives charges — the
// pending->processing move here is what a redelivered refund task's second
// (or Nth) delivery loses.
func (r *PGRepository) ApplyRefundTransition(ctx context.Context, params RefundTransitionParams) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("refund: apply transition: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	if params.ProviderEventID != nil {
		var exists bool
		const dedupSQL = `SELECT EXISTS(SELECT 1 FROM refund_events WHERE provider = $1 AND provider_event_id = $2)`
		if err := tx.QueryRow(ctx, dedupSQL, params.Provider, *params.ProviderEventID).Scan(&exists); err != nil {
			return fmt.Errorf("refund: apply transition: dedup check: %w", err)
		}
		if exists {
			return apperror.New(apperror.CodeDuplicateEvent, "refund: provider_event_id already applied")
		}
	}

	const lockSQL = `SELECT status, last_event_at FROM refunds WHERE id = $1 FOR UPDATE`
	var current Status
	var lastEventAt time.Time
	if err := tx.QueryRow(ctx, lockSQL, params.RefundID).Scan(&current, &lastEventAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return apperror.New(apperror.CodeNotFound, "refund: not found")
		}
		return fmt.Errorf("refund: apply transition: lock: %w", err)
	}

	if err := Transition(current, params.To, params.EventTS, lastEventAt); err != nil {
		return err
	}

	const updateSQL = `
		UPDATE refunds
		SET status = $1, last_event_at = $2, updated_at = now(),
		    provider_refund_ref = coalesce($3, provider_refund_ref)
		WHERE id = $4`
	if _, err := tx.Exec(ctx, updateSQL, params.To, params.EventTS, params.ProviderRefundRef, params.RefundID); err != nil {
		return fmt.Errorf("refund: apply transition: update: %w", err)
	}

	const insertEventSQL = `
		INSERT INTO refund_events (refund_id, event_type, provider, provider_event_id, provider_event_ts, raw_payload, initiated_by)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`
	if _, err := tx.Exec(ctx, insertEventSQL,
		params.RefundID, params.EventType, params.Provider, params.ProviderEventID, params.EventTS, params.RawPayload, params.InitiatedBy,
	); err != nil {
		return fmt.Errorf("refund: apply transition: insert event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("refund: apply transition: commit: %w", err)
	}
	return nil
}

const refundSelectColumns = `id, payment_id, tenant_id, livemode, currency, amount, operation_type, provider,
		       provider_ref, provider_refund_ref, status, last_event_at, created_at, updated_at,
		       source_app, channel, trace_id, description, initiated_by, request_ip, request_user_agent`

func refundScanDest(ref *Refund) []any {
	return []any{
		&ref.ID, &ref.PaymentID, &ref.TenantID, &ref.Livemode, &ref.Currency, &ref.Amount, &ref.OperationType, &ref.Provider,
		&ref.ProviderRef, &ref.ProviderRefundRef, &ref.Status, &ref.LastEventAt, &ref.CreatedAt, &ref.UpdatedAt,
		&ref.SourceApp, &ref.Channel, &ref.TraceID, &ref.Description, &ref.InitiatedBy, &ref.RequestIP, &ref.RequestUserAgent,
	}
}

// GetRefund fetches a refund by id only, with NO tenant scoping -- for
// internal (non-caller-facing) callers, mirroring Repository.Get's doc
// exactly. Never call this from a REST/gRPC handler serving an external
// request -- see GetRefundForTenant.
func (r *PGRepository) GetRefund(ctx context.Context, id uuid.UUID) (*Refund, error) {
	selectSQL := `SELECT ` + refundSelectColumns + ` FROM refunds WHERE id = $1`

	ref := &Refund{}
	err := r.pool.QueryRow(ctx, selectSQL, id).Scan(refundScanDest(ref)...)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, apperror.New(apperror.CodeNotFound, "refund: not found")
		}
		return nil, fmt.Errorf("refund: get: %w", err)
	}
	return ref, nil
}

// GetRefundForTenant is GetRefund's tenant-scoped counterpart -- mirrors
// Repository.GetForTenant's doc exactly (same not-found-not-leak guarantee).
func (r *PGRepository) GetRefundForTenant(ctx context.Context, id uuid.UUID, tenantID string) (*Refund, error) {
	selectSQL := `SELECT ` + refundSelectColumns + ` FROM refunds WHERE id = $1 AND tenant_id = $2`

	ref := &Refund{}
	err := r.pool.QueryRow(ctx, selectSQL, id, tenantID).Scan(refundScanDest(ref)...)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, apperror.New(apperror.CodeNotFound, "refund: not found")
		}
		return nil, fmt.Errorf("refund: get for tenant: %w", err)
	}
	return ref, nil
}

// FindRefundByProviderRefundRef looks up a refund by (provider,
// providerRefundRef) -- the refund-table analogue of
// Repository.FindByProviderRef, used to join an inbound webhook keyed by a
// refund's OWN async-completion reference. Deliberately NOT
// refunds.provider_ref (that column is the ORIGINAL charge's ref, copied
// from the parent payment by CreateRefundWithOutbox -- looking that up here
// would collide with the payment lookup for the same ref).
func (r *PGRepository) FindRefundByProviderRefundRef(ctx context.Context, provider, providerRefundRef string) (*Refund, error) {
	selectSQL := `SELECT ` + refundSelectColumns + ` FROM refunds WHERE provider = $1 AND provider_refund_ref = $2`

	ref := &Refund{}
	err := r.pool.QueryRow(ctx, selectSQL, provider, providerRefundRef).Scan(refundScanDest(ref)...)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, apperror.New(apperror.CodeNotFound, "refund: not found for provider refund ref")
		}
		return nil, fmt.Errorf("refund: find by provider refund ref: %w", err)
	}
	return ref, nil
}

// SetRefundProviderRef persists provider_refund_ref without changing status
// -- mirrors Repository.SetProviderRef/PayoutRepository.SetPayoutProviderRef
// exactly, for the identical reason: a provider call that returns a refund
// reference but leaves the refund in its current state (still
// "processing"/"unknown") cannot go through ApplyRefundTransition, which
// rejects a same-status "transition" as invalid -- without this, a refund
// whose provider call legitimately returns "processing" has no persisted
// reference for reconciliation to query later.
func (r *PGRepository) SetRefundProviderRef(ctx context.Context, id uuid.UUID, providerRefundRef string) error {
	const updateSQL = `UPDATE refunds SET provider_refund_ref = $1, updated_at = now() WHERE id = $2`
	tag, err := r.pool.Exec(ctx, updateSQL, providerRefundRef, id)
	if err != nil {
		return fmt.Errorf("refund: set provider refund ref: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return apperror.New(apperror.CodeNotFound, "refund: not found")
	}
	return nil
}

// ListRefundsProcessingOlderThan returns refunds in StatusProcessing whose
// last_event_at is older than cutoff -- the refund analogue of
// ListProcessingOlderThan/ListPayoutsProcessingOlderThan, backing the
// reconciliation sweep for stuck refunds: a refund whose provider call
// legitimately returns "processing" needs the identical stuck-processing
// recovery a stuck charge or payout gets, not a permanent dead end.
func (r *PGRepository) ListRefundsProcessingOlderThan(ctx context.Context, cutoff time.Time) ([]*Refund, error) {
	selectSQL := `SELECT ` + refundSelectColumns + `
		FROM refunds
		WHERE status = 'processing' AND last_event_at < $1
		ORDER BY last_event_at`

	rows, err := r.pool.Query(ctx, selectSQL, cutoff)
	if err != nil {
		return nil, fmt.Errorf("refund: list processing older than: %w", err)
	}
	defer rows.Close()

	var out []*Refund
	for rows.Next() {
		ref := &Refund{}
		if err := rows.Scan(refundScanDest(ref)...); err != nil {
			return nil, fmt.Errorf("refund: list processing older than: scan: %w", err)
		}
		out = append(out, ref)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("refund: list processing older than: %w", err)
	}
	return out, nil
}

// RefundRepository is the subset of refund-related persistence Service
// depends on, declared separately from Repository (which stays payment-only
// in its doc comment / historical shape) so this file is the single place
// that grows if refunds ever need their own concrete type — Service takes
// both interfaces satisfied by the same *PGRepository.
type RefundRepository interface {
	CreateRefundWithOutbox(ctx context.Context, ref *Refund, withTx func(ctx context.Context, tx pgx.Tx) error) error
	ApplyRefundTransition(ctx context.Context, params RefundTransitionParams) error
	GetRefund(ctx context.Context, id uuid.UUID) (*Refund, error)
	GetRefundForTenant(ctx context.Context, id uuid.UUID, tenantID string) (*Refund, error)
	FindRefundByProviderRefundRef(ctx context.Context, provider, providerRefundRef string) (*Refund, error)
	SetRefundProviderRef(ctx context.Context, id uuid.UUID, providerRefundRef string) error
	ListRefundsProcessingOlderThan(ctx context.Context, cutoff time.Time) ([]*Refund, error)
}

var _ RefundRepository = (*PGRepository)(nil)
