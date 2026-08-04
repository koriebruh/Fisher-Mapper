// Package webhook implements the "webhook arrives before our transaction
// commits" case (plan Decide Now item 9): the incoming_webhook_events
// staging table (schema shipped in Fase 1's migration 00001) plus the
// no-404 rule -- a webhook for a provider_ref that has no payment row yet
// is staged, not rejected, and the caller always acknowledges it. Join
// re-applies staged events once a matching payment row (and its
// provider_ref) exists.
package webhook

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"Fisher-Mapper/internal/apperror"
	"Fisher-Mapper/internal/provider"
)

// StagedEvent is a row from incoming_webhook_events.
type StagedEvent struct {
	ID          uuid.UUID
	Provider    string
	EventID     string
	ProviderRef string
	Payload     []byte
}

// Store persists staged webhook events.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore builds a Store over an existing pool.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Stage inserts a staging row, deduplicating on the (provider, event_id,
// provider_ref) unique index (migration 00001) via ON CONFLICT DO NOTHING
// -- a provider redelivering the same event before it's been processed
// must not create a second staged copy.
func (s *Store) Stage(ctx context.Context, providerName, eventID, providerRef string, payload []byte) error {
	const insertSQL = `
		INSERT INTO incoming_webhook_events (provider, event_id, provider_ref, payload)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (provider, event_id, provider_ref) DO NOTHING`
	if _, err := s.pool.Exec(ctx, insertSQL, providerName, eventID, providerRef, payload); err != nil {
		return fmt.Errorf("webhook: stage: %w", err)
	}
	return nil
}

// FindUnprocessedByRef returns staged events for (providerName,
// providerRef) that have not yet been joined to a payment.
func (s *Store) FindUnprocessedByRef(ctx context.Context, providerName, providerRef string) ([]StagedEvent, error) {
	const selectSQL = `
		SELECT id, provider, event_id, coalesce(provider_ref, '')
		     , payload
		FROM incoming_webhook_events
		WHERE provider = $1 AND provider_ref = $2 AND processed_at IS NULL`

	rows, err := s.pool.Query(ctx, selectSQL, providerName, providerRef)
	if err != nil {
		return nil, fmt.Errorf("webhook: find unprocessed: %w", err)
	}
	defer rows.Close()

	var out []StagedEvent
	for rows.Next() {
		var e StagedEvent
		if err := rows.Scan(&e.ID, &e.Provider, &e.EventID, &e.ProviderRef, &e.Payload); err != nil {
			return nil, fmt.Errorf("webhook: find unprocessed: scan: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("webhook: find unprocessed: %w", err)
	}
	return out, nil
}

// MarkProcessed records that a staged event has been joined to paymentID.
func (s *Store) MarkProcessed(ctx context.Context, id, paymentID uuid.UUID) error {
	const updateSQL = `UPDATE incoming_webhook_events SET processed_at = now(), payment_id = $1 WHERE id = $2`
	if _, err := s.pool.Exec(ctx, updateSQL, paymentID, id); err != nil {
		return fmt.Errorf("webhook: mark processed: %w", err)
	}
	return nil
}

// ParseFunc turns a staged event's raw payload back into a
// provider.WebhookEvent -- normally a closure over the provider's
// ParseWebhook (headers are not preserved in staging, only the body, which
// is all the mock provider's ParseWebhook needs).
type ParseFunc func(ctx context.Context, payload []byte) (provider.WebhookEvent, error)

// ApplyFunc applies a parsed event to the payment it refers to -- normally
// payment.Service.ApplyProviderEvent.
type ApplyFunc func(ctx context.Context, providerName string, evt provider.WebhookEvent) error

// benignApplyOutcome reports whether err from ApplyFunc means "this staged
// event has been resolved, stop trying it again" even though it wasn't a
// state transition (already applied, stale, or the payment is terminal) --
// as opposed to a genuine, retryable infrastructure error.
func benignApplyOutcome(err error) bool {
	if err == nil {
		return true
	}
	switch apperror.CodeOf(err) {
	case apperror.CodeDuplicateEvent, apperror.CodeStaleEvent, apperror.CodeTerminalState:
		return true
	default:
		return false
	}
}

// Join looks up staged events for (providerName, providerRef) and applies
// each via apply, using parse to decode the staged raw payload. Called once
// a payment row's provider_ref becomes known -- per plan item 9, "webhook
// telat gak boleh nimpa state yang lebih baru" is still enforced by apply
// itself (the state machine), Join is only responsible for finding events
// that arrived too early and giving them a second chance.
//
// Returns the count of events resolved (applied or found benign) so callers
// can log/test against it. A genuine apply error leaves that event
// unprocessed for a future Join call to retry, and does not fail the whole
// batch.
func Join(
	ctx context.Context,
	store *Store,
	parse ParseFunc,
	apply ApplyFunc,
	providerName, providerRef string,
	paymentID uuid.UUID,
) (resolved int, err error) {
	events, err := store.FindUnprocessedByRef(ctx, providerName, providerRef)
	if err != nil {
		return 0, err
	}

	for _, e := range events {
		evt, perr := parse(ctx, e.Payload)
		if perr != nil {
			slog.Error("webhook join: parse staged payload failed", "error", perr, "staged_id", e.ID)
			continue
		}

		aerr := apply(ctx, providerName, evt)
		if !benignApplyOutcome(aerr) {
			slog.Error("webhook join: apply failed, will retry on next join", "error", aerr, "staged_id", e.ID)
			continue
		}

		if merr := store.MarkProcessed(ctx, e.ID, paymentID); merr != nil {
			slog.Error("webhook join: mark processed failed", "error", merr, "staged_id", e.ID)
			continue
		}
		resolved++
	}

	return resolved, nil
}
