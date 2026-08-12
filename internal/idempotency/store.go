// Package idempotency implements the Idempotency-Key store: atomic insert
// on a unique (tenant_id, scope, idempotency_key) constraint — never
// check-then-insert, so two concurrent requests with the same key can never
// both believe they "own" the request. See migration 00002 for the base
// table definition and migration 00005 for the scope column (Fase 4: "Refund
// idempotency scope sendiri (distinct dari charge idempotency)" — without a
// scope, a charge and a refund issued by the same tenant that happen to
// reuse the same key string would collide).
package idempotency

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// State describes the outcome of a Reserve call.
type State int

const (
	// StateReserved means the caller now owns this key: no prior row
	// existed, and Reserve just atomically inserted one in status
	// "reserved". The caller must do the work and call Complete.
	StateReserved State = iota

	// StateCompleted means a row already existed with a matching
	// fingerprint and status "completed" — the caller should replay the
	// stored response rather than redo the work.
	StateCompleted

	// StateInProgress means a row already exists with a matching
	// fingerprint but is still "reserved" (another concurrent request owns
	// it and hasn't finished yet).
	StateInProgress

	// StateConflict means a row already exists for this key with a
	// DIFFERENT fingerprint — same Idempotency-Key, different request
	// body. Per plan: this is a 409.
	StateConflict
)

// ScopeCharge and ScopeRefund are the two idempotency scopes Fase 4
// distinguishes. Declared here (not in package payment) so this package's
// call sites are self-documenting without importing payment for a string
// constant.
const (
	ScopeCharge = "charge"
	ScopeRefund = "refund"
)

type Record struct {
	TenantID           string
	Scope              string
	Key                string
	FingerprintHash    string
	Status             string
	ResponseStatusCode int
	ResponseBody       []byte
}

type ReserveResult struct {
	State  State
	Record *Record
}

// Store is the idempotency-key persistence interface. scope (ScopeCharge /
// ScopeRefund) is part of the identity of a key everywhere it appears —
// two different scopes with the same (tenantID, key) never collide.
type Store interface {
	// Reserve atomically inserts a "reserved" row for (tenantID, scope, key)
	// if none exists, or reports the existing row's relationship to
	// fingerprintHash (conflict / in-progress / completed) if one does.
	Reserve(ctx context.Context, tenantID, scope, key, fingerprintHash string) (ReserveResult, error)

	// Complete marks a reserved row as completed, storing the response so
	// future replays with the same key+fingerprint return it verbatim.
	Complete(ctx context.Context, tenantID, scope, key string, statusCode int, body []byte) error

	// Get fetches the current row for (tenantID, scope, key), used by
	// callers polling for another goroutine's in-progress reservation to
	// finish.
	Get(ctx context.Context, tenantID, scope, key string) (*Record, error)
}

// PGStore is the pgx-backed Store implementation.
type PGStore struct {
	pool *pgxpool.Pool
}

func NewPGStore(pool *pgxpool.Pool) *PGStore {
	return &PGStore{pool: pool}
}

func (s *PGStore) Reserve(ctx context.Context, tenantID, scope, key, fingerprintHash string) (ReserveResult, error) {
	const insertSQL = `
		INSERT INTO idempotency_keys (tenant_id, scope, idempotency_key, fingerprint_hash, status)
		VALUES ($1, $2, $3, $4, 'reserved')
		ON CONFLICT (tenant_id, scope, idempotency_key) DO NOTHING
		RETURNING id`

	var id string
	err := s.pool.QueryRow(ctx, insertSQL, tenantID, scope, key, fingerprintHash).Scan(&id)
	if err == nil {
		// We won the atomic insert: nobody else has this key yet.
		return ReserveResult{State: StateReserved}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return ReserveResult{}, fmt.Errorf("idempotency: reserve insert: %w", err)
	}

	// ON CONFLICT DO NOTHING produced zero rows: a row already exists.
	// Fetch it to decide conflict vs. in-progress vs. completed.
	rec, err := s.Get(ctx, tenantID, scope, key)
	if err != nil {
		return ReserveResult{}, err
	}

	if rec.FingerprintHash != fingerprintHash {
		return ReserveResult{State: StateConflict, Record: rec}, nil
	}
	if rec.Status == "completed" {
		return ReserveResult{State: StateCompleted, Record: rec}, nil
	}
	return ReserveResult{State: StateInProgress, Record: rec}, nil
}

func (s *PGStore) Complete(ctx context.Context, tenantID, scope, key string, statusCode int, body []byte) error {
	const updateSQL = `
		UPDATE idempotency_keys
		SET status = 'completed', response_status_code = $1, response_body = $2, updated_at = now()
		WHERE tenant_id = $3 AND scope = $4 AND idempotency_key = $5`

	tag, err := s.pool.Exec(ctx, updateSQL, statusCode, body, tenantID, scope, key)
	if err != nil {
		return fmt.Errorf("idempotency: complete: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("idempotency: complete: no row for tenant=%q scope=%q key=%q", tenantID, scope, key)
	}
	return nil
}

func (s *PGStore) Get(ctx context.Context, tenantID, scope, key string) (*Record, error) {
	const selectSQL = `
		SELECT tenant_id, scope, idempotency_key, fingerprint_hash, status,
		       coalesce(response_status_code, 0), coalesce(response_body, '{}'::jsonb)
		FROM idempotency_keys
		WHERE tenant_id = $1 AND scope = $2 AND idempotency_key = $3`

	var rec Record
	err := s.pool.QueryRow(ctx, selectSQL, tenantID, scope, key).Scan(
		&rec.TenantID, &rec.Scope, &rec.Key, &rec.FingerprintHash, &rec.Status,
		&rec.ResponseStatusCode, &rec.ResponseBody,
	)
	if err != nil {
		return nil, fmt.Errorf("idempotency: get: %w", err)
	}
	return &rec, nil
}

var _ Store = (*PGStore)(nil)
