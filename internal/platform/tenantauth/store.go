// Package tenantauth is the "Stub Cheap" tenant-authentication tier (per
// plan classification): a static per-tenant API key, looked up against the
// tenant_api_keys table, resolving to the caller's authenticated tenant_id.
// Deliberately as simple as internal/provider/auth's outbound Signer -- a
// single credential per tenant, no scopes/roles/expiry -- since the whole
// point of a stub-cheap mechanism is that it is cheap to rip out and
// replace with real OAuth2/JWT/mTLS later, not that it is complete.
package tenantauth

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"Fisher-Mapper/internal/domain/apperror"
)

// Store resolves an API key to the tenant_id it was issued to.
type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Resolve looks up apiKey and returns the tenant_id it belongs to. Any
// failure -- empty key, unknown key, or a DB error -- comes back as
// apperror.CodeUnauthorized (a DB error included: for the exact reason
// admin.go's adminAuth fails closed, an auth check that can't reach its
// backing store must reject rather than let the caller through
// unauthenticated) EXCEPT the raw DB error case, which is wrapped instead so
// callers/logs can still tell "no such key" apart from "store unreachable".
func (s *Store) Resolve(ctx context.Context, apiKey string) (string, error) {
	if apiKey == "" {
		return "", apperror.New(apperror.CodeUnauthorized, "tenantauth: missing api key")
	}

	var tenantID string
	const selectSQL = `SELECT tenant_id FROM tenant_api_keys WHERE api_key = $1`
	err := s.pool.QueryRow(ctx, selectSQL, apiKey).Scan(&tenantID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", apperror.New(apperror.CodeUnauthorized, "tenantauth: unknown api key")
		}
		return "", fmt.Errorf("tenantauth: resolve: %w", err)
	}
	return tenantID, nil
}

// CreateKey generates a fresh random API key (32 bytes, hex-encoded --
// never derived from tenantID or anything guessable) and inserts it for
// tenantID, returning the plaintext key. This is the ONLY way a key comes
// into existence (see migration 00010's doc for why there is no seed row):
// cmd/migrate's -create-tenant-key flag is the intended caller, printing the
// result once to an operator's terminal, never committed anywhere.
//
// Stored in plaintext (no hash-at-rest) -- an explicit, documented
// simplification consistent with this being the cheapest tier: a real
// deployment replacing this stub should hash the key the same way a
// password would be hashed, and this table/Store is deliberately narrow
// enough that swapping the storage format later touches only this file.
func (s *Store) CreateKey(ctx context.Context, tenantID string) (string, error) {
	if tenantID == "" {
		return "", apperror.New(apperror.CodeValidation, "tenantauth: tenant_id is required")
	}

	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("tenantauth: generate key: %w", err)
	}
	apiKey := hex.EncodeToString(raw)

	const insertSQL = `INSERT INTO tenant_api_keys (api_key, tenant_id) VALUES ($1, $2)`
	if _, err := s.pool.Exec(ctx, insertSQL, apiKey, tenantID); err != nil {
		return "", fmt.Errorf("tenantauth: create key: %w", err)
	}
	return apiKey, nil
}
