-- +goose Up

-- Phase 2 needs the columns the state machine and webhook dedup logic
-- actually read/write, which Phase 1 deliberately left as a schema-only
-- placeholder (see comments in 00001_init_payments.sql).

-- last_event_at tracks the timestamp of the last event applied to this
-- payment (internal transition or provider webhook event). The stale-event
-- guard (statemachine.Transition) compares an incoming event's timestamp
-- against this column — without it there is nothing to reject an
-- out-of-order event against.
ALTER TABLE payments ADD COLUMN last_event_at timestamptz NOT NULL DEFAULT now();

-- provider_event_ts is the PROVIDER's event timestamp (as opposed to
-- created_at, which is when *we* received/recorded it) — this is the value
-- compared against payments.last_event_at for stale-event rejection.
ALTER TABLE payment_events ADD COLUMN provider_event_ts timestamptz;

-- provider was missing from payment_events entirely in Phase 1. Dedup must
-- be scoped per provider (two different PJPs could coincidentally reuse an
-- event id), matching the (provider, provider_ref) precedent already on
-- payments.
ALTER TABLE payment_events ADD COLUMN provider text NOT NULL DEFAULT '';
ALTER TABLE payment_events ALTER COLUMN provider DROP DEFAULT;

-- Dedup key (Decide Now item 7 / Phase 2 explicit requirement): an event
-- with the same (provider, provider_event_id) must never be applied twice.
-- Partial index (WHERE provider_event_id IS NOT NULL) because internal,
-- non-webhook-driven transitions (e.g. pending -> processing right after
-- creation) legitimately have no provider event id and must not collide
-- with each other under a plain unique index.
CREATE UNIQUE INDEX payment_events_provider_event_dedup_idx
    ON payment_events (provider, provider_event_id)
    WHERE provider_event_id IS NOT NULL;

-- idempotency_keys backs internal/idempotency.Store: atomic insert on a
-- unique constraint (never check-then-insert), so two concurrent requests
-- with the same key can never both believe they own the request. Scoped by
-- tenant_id, consistent with every other transaction-adjacent table having
-- tenant_id from day one.
CREATE TABLE idempotency_keys (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id               text NOT NULL,
    idempotency_key         text NOT NULL,
    fingerprint_hash        text NOT NULL,
    status                  text NOT NULL CHECK (status IN ('reserved', 'completed')),
    response_status_code    int,
    response_body           jsonb,
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idempotency_keys_tenant_key_idx
    ON idempotency_keys (tenant_id, idempotency_key);

-- +goose Down

DROP TABLE IF EXISTS idempotency_keys;

DROP INDEX IF EXISTS payment_events_provider_event_dedup_idx;
ALTER TABLE payment_events DROP COLUMN IF EXISTS provider;
ALTER TABLE payment_events DROP COLUMN IF EXISTS provider_event_ts;

ALTER TABLE payments DROP COLUMN IF EXISTS last_event_at;
