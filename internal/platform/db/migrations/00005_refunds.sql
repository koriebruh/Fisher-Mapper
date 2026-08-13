-- +goose Up

-- refunds is the Fase 4 "Refund idempotency scope sendiri" table (plan
-- Decide Now item 10). Modeled as its own table (not another payments row)
-- because a refund needs its own state machine instance (pending ->
-- processing -> succeeded|failed, same graph as payments, applied via the
-- same internal/domain/payment.Transition pure function) independent of the
-- original charge's -- a payment stays "succeeded" even while partially
-- refunded.
CREATE TABLE refunds (
    id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    payment_id              uuid NOT NULL REFERENCES payments (id),

    tenant_id               text NOT NULL,
    livemode                boolean NOT NULL DEFAULT false,
    currency                text NOT NULL CHECK (char_length(currency) = 3),
    amount                  bigint NOT NULL CHECK (amount > 0),

    provider                text NOT NULL,
    -- provider_ref of the ORIGINAL charge being refunded (what
    -- provider.RefundRequest.ProviderRef needs) -- captured at refund-create
    -- time so ProcessRefund never has to re-fetch the payment row.
    provider_ref            text,
    -- provider_refund_ref: the reference the provider assigns to THIS
    -- refund operation (provider.RefundResponse.ProviderRefundRef).
    provider_refund_ref     text,

    status                  text NOT NULL DEFAULT 'pending' CHECK (status IN (
                                'pending', 'processing', 'succeeded', 'failed'
                            )),
    last_event_at           timestamptz NOT NULL DEFAULT now(),

    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX refunds_payment_id_idx ON refunds (payment_id);
CREATE INDEX refunds_status_idx ON refunds (status);

-- refund_events mirrors payment_events (Decide Now item 8) for the same
-- append-only audit/dispute reason, scoped to refunds instead of payments.
CREATE TABLE refund_events (
    id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    refund_id           uuid NOT NULL REFERENCES refunds (id),
    event_type          text NOT NULL,
    provider            text NOT NULL,
    provider_event_id   text,
    provider_event_ts   timestamptz,
    raw_payload         jsonb,
    created_at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX refund_events_refund_id_idx ON refund_events (refund_id);

CREATE UNIQUE INDEX refund_events_provider_event_dedup_idx
    ON refund_events (provider, provider_event_id)
    WHERE provider_event_id IS NOT NULL;

-- Idempotency scope (plan: "Refund idempotency scope sendiri (distinct dari
-- charge idempotency)"). idempotency_keys previously deduped on
-- (tenant_id, idempotency_key) alone, which would let a charge and a refund
-- issued by the same tenant with the same key string collide. Existing rows
-- backfill to 'charge' -- every idempotency key created before this
-- migration came from the charge flow (the only one that existed).
ALTER TABLE idempotency_keys ADD COLUMN scope text NOT NULL DEFAULT 'charge';
ALTER TABLE idempotency_keys ALTER COLUMN scope DROP DEFAULT;

DROP INDEX IF EXISTS idempotency_keys_tenant_key_idx;
CREATE UNIQUE INDEX idempotency_keys_tenant_scope_key_idx
    ON idempotency_keys (tenant_id, scope, idempotency_key);

-- +goose Down

DROP INDEX IF EXISTS idempotency_keys_tenant_scope_key_idx;
CREATE UNIQUE INDEX idempotency_keys_tenant_key_idx
    ON idempotency_keys (tenant_id, idempotency_key);
ALTER TABLE idempotency_keys DROP COLUMN IF EXISTS scope;

DROP TABLE IF EXISTS refund_events;
DROP TABLE IF EXISTS refunds;
