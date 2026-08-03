-- +goose Up

-- payments is the core transaction table. Every column here is locked in by
-- the "Invarian Uang — Decide Now" section of the project plan: adding any
-- of tenant_id / livemode / currency / operation_type / provider_ref later,
-- once rows exist, would be a breaking data migration instead of a cheap
-- config change.
CREATE TABLE payments (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),

    -- multi-tenancy from day one; retrofitting this onto a populated table
    -- is a large, risky data migration.
    tenant_id       text NOT NULL,

    -- test vs. production traffic must never be ambiguous.
    livemode        boolean NOT NULL DEFAULT false,

    -- ISO 4217 currency code, no implicit default — every write must be
    -- explicit about which currency it means.
    currency        text NOT NULL CHECK (char_length(currency) = 3),

    -- minor unit (e.g. cents), int64/bigint — never a float for money.
    amount          bigint NOT NULL CHECK (amount >= 0),

    operation_type  text NOT NULL CHECK (operation_type IN (
                        'charge', 'authorize', 'capture', 'refund',
                        'payout', 'reversal'
                    )),

    provider        text NOT NULL,

    -- nullable: a payment can exist before the provider has assigned a
    -- reference (e.g. between "create" and the first provider call).
    -- Postgres unique indexes treat each NULL as distinct, so multiple
    -- provider_ref-less rows never spuriously collide.
    provider_ref    text,

    -- explicit state machine, see plan section "Invarian Uang — Decide Now"
    -- item 7. Terminal states (succeeded/failed) are enforced immutable at
    -- the application layer in a later phase (SELECT ... FOR UPDATE), not
    -- by a DB trigger here.
    status          text NOT NULL DEFAULT 'pending' CHECK (status IN (
                        'pending', 'processing', 'succeeded', 'failed'
                    )),

    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

-- Dedup key for webhook matching & reconciliation (Decide Now item 5).
CREATE UNIQUE INDEX payments_provider_provider_ref_idx
    ON payments (provider, provider_ref);

CREATE INDEX payments_tenant_id_idx ON payments (tenant_id);
CREATE INDEX payments_status_idx ON payments (status);

-- payment_events: append-only audit/dispute trail (Decide Now item 8).
-- Schema-only placeholder in Phase 1 — the transition logic, the
-- provider_event_id uniqueness/timestamp-window enforcement, and the
-- append-only guard are Phase 2 work. The column shapes are locked in now
-- because payment_events is part of the same irreversible schema family as
-- payments (adding columns later after rows exist is the same class of
-- breaking migration).
CREATE TABLE payment_events (
    id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    payment_id          uuid NOT NULL REFERENCES payments (id),
    event_type          text NOT NULL,
    provider_event_id   text,
    raw_payload         jsonb,
    created_at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX payment_events_payment_id_idx ON payment_events (payment_id);

-- incoming_webhook_events: staging table for the "webhook arrives before
-- our transaction commits" case (Decide Now item 9). payment_id is
-- nullable on purpose: the reconciler joins staged events to a payment row
-- once it exists. Schema-only placeholder in Phase 1 — the no-404 handler
-- and reconciler join logic are Phase 3 work.
CREATE TABLE incoming_webhook_events (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    provider        text NOT NULL,
    event_id        text NOT NULL,
    provider_ref    text,
    payment_id      uuid REFERENCES payments (id),
    payload         jsonb NOT NULL,
    received_at     timestamptz NOT NULL DEFAULT now(),
    processed_at    timestamptz
);

CREATE UNIQUE INDEX incoming_webhook_events_dedup_idx
    ON incoming_webhook_events (provider, event_id, provider_ref);

CREATE INDEX incoming_webhook_events_payment_id_idx
    ON incoming_webhook_events (payment_id);

-- +goose Down

DROP TABLE IF EXISTS incoming_webhook_events;
DROP TABLE IF EXISTS payment_events;
DROP TABLE IF EXISTS payments;
