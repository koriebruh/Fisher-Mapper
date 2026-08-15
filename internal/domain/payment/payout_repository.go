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

// PayoutTransitionParams bundles the arguments for
// Repository.ApplyPayoutTransition -- the payout-table analogue of
// TransitionParams/RefundTransitionParams.
type PayoutTransitionParams struct {
	PayoutID  uuid.UUID
	To        Status
	EventTS   time.Time
	EventType string
	Provider  string

	// ProviderRef, if non-nil, is written to payouts.provider_ref as part of
	// the same update -- the reference the provider assigned on the call
	// that drove this transition.
	ProviderRef *string

	ProviderEventID *string
	RawPayload      []byte

	// InitiatedBy is the per-TRANSITION actor -- see TransitionParams.InitiatedBy's
	// doc for the full taxonomy. Every call site must set this explicitly.
	InitiatedBy string
}

// CreatePayoutWithOutbox inserts a new payout row (status pending) and, in
// the SAME transaction, invokes withTx (e.g. to insert an outbox row) --
// mirrors Repository.CreateWithOutbox exactly. Unlike
// CreateRefundWithOutbox, there is no parent row to lock: a payout is
// standalone, so this is a plain insert, not a locked read-then-insert.
func (r *PGRepository) CreatePayoutWithOutbox(ctx context.Context, p *Payout, withTx func(ctx context.Context, tx pgx.Tx) error) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("payout: create with outbox: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	const insertSQL = `
		INSERT INTO payouts (tenant_id, livemode, currency, amount, provider, destination, status,
		    source_app, channel, trace_id, description, initiated_by, request_ip, request_user_agent)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		RETURNING id, status, last_event_at, created_at, updated_at`

	err = tx.QueryRow(ctx, insertSQL,
		p.TenantID, p.Livemode, p.Currency, p.Amount, p.Provider, p.Destination, StatusPending,
		p.SourceApp, p.Channel, p.TraceID, p.Description, p.InitiatedBy, p.RequestIP, p.RequestUserAgent,
	).Scan(&p.ID, &p.Status, &p.LastEventAt, &p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		return fmt.Errorf("payout: create with outbox: insert payout: %w", err)
	}

	if err := withTx(ctx, tx); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("payout: create with outbox: commit: %w", err)
	}
	return nil
}

// ApplyPayoutTransition is the payout-table analogue of ApplyTransition /
// ApplyRefundTransition: identical dedup / row-lock / Transition / update /
// append-only-event / commit shape, applied to payouts + payout_events.
// This is what gives ProcessPayout's pending->processing move the same
// redelivery/double-dispatch safety ProcessCharge/ProcessRefund get from
// their own CAS.
func (r *PGRepository) ApplyPayoutTransition(ctx context.Context, params PayoutTransitionParams) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("payout: apply transition: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	if params.ProviderEventID != nil {
		var exists bool
		const dedupSQL = `SELECT EXISTS(SELECT 1 FROM payout_events WHERE provider = $1 AND provider_event_id = $2)`
		if err := tx.QueryRow(ctx, dedupSQL, params.Provider, *params.ProviderEventID).Scan(&exists); err != nil {
			return fmt.Errorf("payout: apply transition: dedup check: %w", err)
		}
		if exists {
			return apperror.New(apperror.CodeDuplicateEvent, "payout: provider_event_id already applied")
		}
	}

	const lockSQL = `SELECT status, last_event_at FROM payouts WHERE id = $1 FOR UPDATE`
	var current Status
	var lastEventAt time.Time
	if err := tx.QueryRow(ctx, lockSQL, params.PayoutID).Scan(&current, &lastEventAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return apperror.New(apperror.CodeNotFound, "payout: not found")
		}
		return fmt.Errorf("payout: apply transition: lock: %w", err)
	}

	if err := Transition(current, params.To, params.EventTS, lastEventAt); err != nil {
		return err
	}

	const updateSQL = `
		UPDATE payouts
		SET status = $1, last_event_at = $2, updated_at = now(),
		    provider_ref = coalesce($3, provider_ref)
		WHERE id = $4`
	if _, err := tx.Exec(ctx, updateSQL, params.To, params.EventTS, params.ProviderRef, params.PayoutID); err != nil {
		return fmt.Errorf("payout: apply transition: update: %w", err)
	}

	const insertEventSQL = `
		INSERT INTO payout_events (payout_id, event_type, provider, provider_event_id, provider_event_ts, raw_payload, initiated_by)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`
	if _, err := tx.Exec(ctx, insertEventSQL,
		params.PayoutID, params.EventType, params.Provider, params.ProviderEventID, params.EventTS, params.RawPayload, params.InitiatedBy,
	); err != nil {
		return fmt.Errorf("payout: apply transition: insert event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("payout: apply transition: commit: %w", err)
	}
	return nil
}

// SetPayoutProviderRef persists provider_ref without changing status --
// mirrors Repository.SetProviderRef exactly, for the identical reason: a
// provider call that returns a reference but leaves the payout in its
// current state (still "processing"/"unknown") cannot go through
// ApplyPayoutTransition, which rejects a same-status "transition" as
// invalid.
func (r *PGRepository) SetPayoutProviderRef(ctx context.Context, id uuid.UUID, providerRef string) error {
	const updateSQL = `UPDATE payouts SET provider_ref = $1, updated_at = now() WHERE id = $2`
	tag, err := r.pool.Exec(ctx, updateSQL, providerRef, id)
	if err != nil {
		return fmt.Errorf("payout: set provider ref: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return apperror.New(apperror.CodeNotFound, "payout: not found")
	}
	return nil
}

// payoutSelectColumns/payoutScanDest mirror paymentSelectColumns/
// paymentScanDest -- shared by GetPayout and ListPayoutsProcessingOlderThan
// so the envelope columns can't drift out of sync between the two queries.
const payoutSelectColumns = `id, tenant_id, livemode, currency, amount, provider,
		       provider_ref, destination, status, last_event_at, created_at, updated_at,
		       source_app, channel, trace_id, description, initiated_by, request_ip, request_user_agent`

func payoutScanDest(p *Payout) []any {
	return []any{
		&p.ID, &p.TenantID, &p.Livemode, &p.Currency, &p.Amount, &p.Provider,
		&p.ProviderRef, &p.Destination, &p.Status, &p.LastEventAt, &p.CreatedAt, &p.UpdatedAt,
		&p.SourceApp, &p.Channel, &p.TraceID, &p.Description, &p.InitiatedBy, &p.RequestIP, &p.RequestUserAgent,
	}
}

// GetPayout fetches a payout by id only, with NO tenant scoping -- for
// internal (non-caller-facing) callers, mirroring Repository.Get's doc
// exactly. Never call this from a REST/gRPC handler serving an external
// request -- see GetPayoutForTenant.
func (r *PGRepository) GetPayout(ctx context.Context, id uuid.UUID) (*Payout, error) {
	selectSQL := `SELECT ` + payoutSelectColumns + ` FROM payouts WHERE id = $1`

	p := &Payout{}
	err := r.pool.QueryRow(ctx, selectSQL, id).Scan(payoutScanDest(p)...)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, apperror.New(apperror.CodeNotFound, "payout: not found")
		}
		return nil, fmt.Errorf("payout: get: %w", err)
	}
	return p, nil
}

// GetPayoutForTenant is GetPayout's tenant-scoped counterpart -- mirrors
// Repository.GetForTenant's doc exactly (same not-found-not-leak guarantee).
func (r *PGRepository) GetPayoutForTenant(ctx context.Context, id uuid.UUID, tenantID string) (*Payout, error) {
	selectSQL := `SELECT ` + payoutSelectColumns + ` FROM payouts WHERE id = $1 AND tenant_id = $2`

	p := &Payout{}
	err := r.pool.QueryRow(ctx, selectSQL, id, tenantID).Scan(payoutScanDest(p)...)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, apperror.New(apperror.CodeNotFound, "payout: not found")
		}
		return nil, fmt.Errorf("payout: get for tenant: %w", err)
	}
	return p, nil
}

// FindPayoutByProviderRef looks up a payout by (provider, providerRef) --
// the payout-table analogue of Repository.FindByProviderRef, added (it did
// not exist before) so an inbound webhook keyed by a payout's own
// provider-assigned reference can be joined to it, the same way a charge's
// webhook already is.
func (r *PGRepository) FindPayoutByProviderRef(ctx context.Context, provider, providerRef string) (*Payout, error) {
	selectSQL := `SELECT ` + payoutSelectColumns + ` FROM payouts WHERE provider = $1 AND provider_ref = $2`

	p := &Payout{}
	err := r.pool.QueryRow(ctx, selectSQL, provider, providerRef).Scan(payoutScanDest(p)...)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, apperror.New(apperror.CodeNotFound, "payout: not found for provider ref")
		}
		return nil, fmt.Errorf("payout: find by provider ref: %w", err)
	}
	return p, nil
}

// ListPayoutsProcessingOlderThan returns payouts in StatusProcessing whose
// last_event_at is older than cutoff -- the payout analogue of
// ListProcessingOlderThan, backing the reconciliation sweep for stuck
// payouts (mandatory per the task: payouts must be reconcilable the same
// way stuck charges are, not just documented as a gap).
func (r *PGRepository) ListPayoutsProcessingOlderThan(ctx context.Context, cutoff time.Time) ([]*Payout, error) {
	selectSQL := `SELECT ` + payoutSelectColumns + `
		FROM payouts
		WHERE status = 'processing' AND last_event_at < $1
		ORDER BY last_event_at`

	rows, err := r.pool.Query(ctx, selectSQL, cutoff)
	if err != nil {
		return nil, fmt.Errorf("payout: list processing older than: %w", err)
	}
	defer rows.Close()

	var out []*Payout
	for rows.Next() {
		p := &Payout{}
		if err := rows.Scan(payoutScanDest(p)...); err != nil {
			return nil, fmt.Errorf("payout: list processing older than: scan: %w", err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("payout: list processing older than: %w", err)
	}
	return out, nil
}

// PayoutRepository is the subset of payout-related persistence Service
// depends on, declared separately from Repository/RefundRepository so this
// file is the single place that grows if payouts ever need their own
// concrete type -- Service takes all three interfaces, satisfied by the same
// *PGRepository.
type PayoutRepository interface {
	CreatePayoutWithOutbox(ctx context.Context, p *Payout, withTx func(ctx context.Context, tx pgx.Tx) error) error
	ApplyPayoutTransition(ctx context.Context, params PayoutTransitionParams) error
	SetPayoutProviderRef(ctx context.Context, id uuid.UUID, providerRef string) error
	GetPayout(ctx context.Context, id uuid.UUID) (*Payout, error)
	GetPayoutForTenant(ctx context.Context, id uuid.UUID, tenantID string) (*Payout, error)
	FindPayoutByProviderRef(ctx context.Context, provider, providerRef string) (*Payout, error)
	ListPayoutsProcessingOlderThan(ctx context.Context, cutoff time.Time) ([]*Payout, error)
}

var _ PayoutRepository = (*PGRepository)(nil)
