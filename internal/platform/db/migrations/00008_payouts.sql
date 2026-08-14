-- +goose Up

-- payouts is the "money OUT, standalone" operation the plan's operation_type
-- enum already reserved a value for (item 4) but never built domain logic
-- against: a merchant disbursement/withdrawal, independent of any prior
-- charge -- unlike refunds, which always target an existing payment. Modeled
-- as its own table (not a payments row with operation_type='payout'), same
-- reasoning as refunds getting its own table: its own state machine
-- instance, its own lifecycle, no parent row to lock.
CREATE TABLE payouts (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),

    tenant_id               text NOT NULL,
    livemode                boolean NOT NULL DEFAULT false,
    currency                text NOT NULL CHECK (char_length(currency) = 3),
    amount                  bigint NOT NULL CHECK (amount > 0),

    -- Fixed at 'payout' -- this table only ever carries that one operation
    -- type, but the column is kept (rather than omitted) so payouts reads
    -- consistently alongside payments/refunds, all three of which expose
    -- operation_type per the plan's money-invariant list.
    operation_type          text NOT NULL DEFAULT 'payout' CHECK (operation_type = 'payout'),

    provider                text NOT NULL,
    -- provider_ref: the reference THIS provider assigns to the payout call
    -- itself (there is no "original charge" ref to distinguish it from,
    -- unlike refunds.provider_ref vs refunds.provider_refund_ref).
    provider_ref            text,

    -- destination is an opaque identifier for where the funds go (a
    -- provider-side bank-account/e-wallet token, NOT raw bank/card account
    -- data) -- required, since a payout with no destination is meaningless.
    destination             text NOT NULL,

    status                  text NOT NULL DEFAULT 'pending' CHECK (status IN (
                                'pending', 'processing', 'succeeded', 'failed'
                            )),
    last_event_at           timestamptz NOT NULL DEFAULT now(),

    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now()
);

-- Dedup key for webhook matching & reconciliation, same precedent as
-- payments/refunds (plan Decide Now item 5).
CREATE UNIQUE INDEX payouts_provider_provider_ref_idx ON payouts (provider, provider_ref);

CREATE INDEX payouts_tenant_id_idx ON payouts (tenant_id);
CREATE INDEX payouts_status_idx ON payouts (status);

-- payout_events mirrors payment_events/refund_events (Decide Now item 8):
-- append-only audit/dispute trail, scoped to payouts.
CREATE TABLE payout_events (
    id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    payout_id           uuid NOT NULL REFERENCES payouts (id),
    event_type          text NOT NULL,
    provider            text NOT NULL,
    provider_event_id   text,
    provider_event_ts   timestamptz,
    raw_payload         jsonb,
    created_at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX payout_events_payout_id_idx ON payout_events (payout_id);

CREATE UNIQUE INDEX payout_events_provider_event_dedup_idx
    ON payout_events (provider, provider_event_id)
    WHERE provider_event_id IS NOT NULL;

-- Payout idempotency scope: idempotency_keys.scope is a plain text column
-- with no CHECK constraint (migration 00005 dropped the default and added
-- no allowlist) -- 'payout' needs no migration, just a new constant
-- (internal/messaging/idempotency.ScopePayout).

-- +goose Down

DROP TABLE IF EXISTS payout_events;
DROP TABLE IF EXISTS payouts;
