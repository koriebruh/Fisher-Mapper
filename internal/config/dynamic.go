// Dynamic config (Fase 4): the app_config Postgres table, loaded AFTER the
// bootstrap Postgres connection exists (see package doc in bootstrap.go --
// this file is deliberately kept separate from that one, never merged into
// it: Bootstrap stays the only config source safe to read before any
// connection exists, Cache/DynamicStore need a live pool). Cached in memory
// with periodic background refresh; on a refresh failure, the cache keeps
// serving its last-known-good snapshot and logs a warning rather than
// blocking or crashing the app (plan: "Kalau DB gak bisa diakses pas
// refresh, jatuh balik ke cache terakhir yang valid, bukan error total").
package config

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DynamicStore is the Postgres-backed persistence for app_config /
// app_config_audit.
type DynamicStore struct {
	pool *pgxpool.Pool
}

// NewDynamicStore builds a DynamicStore over an existing pool.
func NewDynamicStore(pool *pgxpool.Pool) *DynamicStore {
	return &DynamicStore{pool: pool}
}

// GetAll returns every key/value pair currently in app_config -- what
// Cache.refresh calls to repopulate its snapshot.
func (s *DynamicStore) GetAll(ctx context.Context) (map[string]string, error) {
	const selectSQL = `SELECT key, value FROM app_config`
	rows, err := s.pool.Query(ctx, selectSQL)
	if err != nil {
		return nil, fmt.Errorf("config: dynamic store: get all: %w", err)
	}
	defer rows.Close()

	out := make(map[string]string)
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, fmt.Errorf("config: dynamic store: get all: scan: %w", err)
		}
		out[k] = v
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("config: dynamic store: get all: %w", err)
	}
	return out, nil
}

// AuditEntry is one row of app_config_audit, returned by SetWithAudit's
// callers (the admin REST handler) mainly for logging/response purposes.
type AuditEntry struct {
	ID        uuid.UUID
	ConfigKey string
	OldValue  *string
	NewValue  string
	ChangedBy string
	ChangedAt time.Time
}

// SetWithAudit is the Fase 4 "stub cheap" admin write path (plan: "Admin
// config audit table ... siapa ubah apa kapan"). In ONE transaction:
//  1. SELECT ... FOR UPDATE the current app_config row for key (if any) --
//     both to serialize concurrent writers of the same key and so old_value
//     below is read under the same lock it is about to be superseded by,
//     never a stale read from outside the transaction.
//  2. UPSERT app_config.
//  3. INSERT app_config_audit with the old_value captured in step 1.
//  4. COMMIT.
//
// A config change without a matching audit row is impossible by
// construction: either both writes commit, or (on any error, including a
// caller-injected one for testing) neither does.
func (s *DynamicStore) SetWithAudit(ctx context.Context, key, newValue, updatedBy string) (AuditEntry, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return AuditEntry{}, fmt.Errorf("config: set with audit: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	var oldValue *string
	const lockSQL = `SELECT value FROM app_config WHERE key = $1 FOR UPDATE`
	var current string
	err = tx.QueryRow(ctx, lockSQL, key).Scan(&current)
	switch {
	case err == nil:
		oldValue = &current
	case errors.Is(err, pgx.ErrNoRows):
		oldValue = nil
	default:
		return AuditEntry{}, fmt.Errorf("config: set with audit: lock current value: %w", err)
	}

	const upsertSQL = `
		INSERT INTO app_config (key, value, updated_by, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (key) DO UPDATE SET value = $2, updated_by = $3, updated_at = now()`
	if _, err := tx.Exec(ctx, upsertSQL, key, newValue, updatedBy); err != nil {
		return AuditEntry{}, fmt.Errorf("config: set with audit: upsert: %w", err)
	}

	entry := AuditEntry{ConfigKey: key, OldValue: oldValue, NewValue: newValue, ChangedBy: updatedBy}
	const insertAuditSQL = `
		INSERT INTO app_config_audit (config_key, old_value, new_value, changed_by)
		VALUES ($1, $2, $3, $4)
		RETURNING id, changed_at`
	if err := tx.QueryRow(ctx, insertAuditSQL, key, oldValue, newValue, updatedBy).Scan(&entry.ID, &entry.ChangedAt); err != nil {
		return AuditEntry{}, fmt.Errorf("config: set with audit: insert audit row: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return AuditEntry{}, fmt.Errorf("config: set with audit: commit: %w", err)
	}
	return entry, nil
}

// ProviderEnabledKey returns the app_config key convention for a provider's
// enabled flag, e.g. "provider.mock.enabled".
func ProviderEnabledKey(providerName string) string {
	return "provider." + providerName + ".enabled"
}

// configSource is the subset of DynamicStore that Cache depends on,
// declared as an interface so the fallback-on-refresh-failure behavior
// (Cache's whole reason for existing) can be unit tested against a fake
// that fails on command, without needing a live Postgres connection.
// *DynamicStore satisfies this structurally.
type configSource interface {
	GetAll(ctx context.Context) (map[string]string, error)
}

// Cache is the in-memory, periodically-refreshed view of app_config. Every
// read (Get/GetBool) is a plain map read under a read lock -- no I/O, no
// blocking on Postgres, per the plan's explicit reason for this cache
// existing at all ("cek flag dari cache di enqueue+worker", never a live
// round-trip per payment).
type Cache struct {
	store           configSource
	refreshInterval time.Duration

	mu     sync.RWMutex
	values map[string]string
}

// NewCache builds a Cache. It starts empty -- call Load once, synchronously,
// before serving any traffic that depends on it (see doc on Load), then
// hand Run to an oklog/run actor for ongoing background refresh.
func NewCache(store configSource, refreshInterval time.Duration) *Cache {
	if refreshInterval <= 0 {
		refreshInterval = 30 * time.Second
	}
	return &Cache{store: store, refreshInterval: refreshInterval, values: make(map[string]string)}
}

// Load performs the initial, synchronous population of the cache. Unlike
// every subsequent refresh (see Run), a failure here is NOT swallowed: at
// t=0 there is no last-known-good snapshot to fall back to, so a caller
// that cannot reach Postgres at startup should fail startup outright --
// consistent with db.NewPool already hard-failing on its own Ping. Callers
// that want the plan's "config.toml cuma nilai seed/default" fallback
// behavior for values still missing after a successful Load get it for
// free from GetBool's defaultValue parameter.
func (c *Cache) Load(ctx context.Context) error {
	values, err := c.store.GetAll(ctx)
	if err != nil {
		return fmt.Errorf("config: cache: initial load: %w", err)
	}
	c.mu.Lock()
	c.values = values
	c.mu.Unlock()
	return nil
}

// Run periodically refreshes the cache until ctx is done -- the actor
// internal/lifecycle.RunnerActor wraps for oklog/run.Group, alongside the
// outbox relay and asynq server. A refresh failure (e.g. Postgres
// unreachable) is logged as a warning and the loop continues on its normal
// interval, still serving whatever snapshot Load (or the last successful
// refresh) populated -- this is the plan's explicit "fallback ke cache
// terakhir yang valid, bukan error total" requirement, and it is why Run
// returning an error at all would defeat the purpose: it never does, except
// by ctx cancellation (which RunnerActor turns into a clean shutdown, not a
// reported error).
func (c *Cache) Run(ctx context.Context) error {
	ticker := time.NewTicker(c.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			c.refresh(ctx)
		}
	}
}

// Refresh performs one refresh cycle immediately, outside Run's ticker --
// used by the admin write path (cmd/server, a different process than the
// worker that actually enforces flags) to shorten propagation latency in
// its OWN process after a write, and by tests. Same fallback behavior as
// Run's periodic call: an error is logged and swallowed, never returned as
// a failure the caller must handle, since a stale cache is always an
// acceptable outcome here.
func (c *Cache) Refresh(ctx context.Context) {
	c.refresh(ctx)
}

func (c *Cache) refresh(ctx context.Context) {
	values, err := c.store.GetAll(ctx)
	if err != nil {
		slog.Warn("config: cache refresh failed, continuing to serve last-known-good snapshot", "error", err)
		return
	}
	c.mu.Lock()
	c.values = values
	c.mu.Unlock()
}

// Get returns the raw string value for key, and whether it was present.
func (c *Cache) Get(key string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.values[key]
	return v, ok
}

// GetBool returns the boolean value for key ("true"/"1"/"t" parse as true
// via strconv.ParseBool), or defaultValue if key is absent or unparsable.
func (c *Cache) GetBool(key string, defaultValue bool) bool {
	v, ok := c.Get(key)
	if !ok {
		return defaultValue
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		slog.Warn("config: cache: value is not a valid bool, using default", "key", key, "value", v)
		return defaultValue
	}
	return b
}

// ProviderEnabled reports whether providerName's provider-enabled flag is
// on -- the Fase 4 first real use of dynamic config. Defaults to true
// (enabled) if the key is entirely absent from app_config, so a provider
// that has never had its flag explicitly set behaves exactly as it did
// before Fase 4 existed.
func (c *Cache) ProviderEnabled(providerName string) bool {
	return c.GetBool(ProviderEnabledKey(providerName), true)
}
