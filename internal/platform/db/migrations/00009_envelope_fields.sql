-- +goose Up

-- Envelope completeness (this template's core purpose is fast-mapping onto
-- a real PJP; a financial transaction record with no request/actor context
-- is not "complete" just because the money invariants are right). Applied
-- identically to payments, refunds and payouts -- the task's explicit
-- requirement that payout carry the SAME completeness as charge/refund, not
-- a stripped-down version.
--
-- source_app / channel are deliberately TWO columns, not one: source_app is
-- a client-supplied identifier for WHICH calling application originated the
-- request (this template may back multiple client apps), channel is set by
-- the TRANSPORT itself ("rest"/"grpc") -- "who is calling" vs "how are they
-- calling" are different facts. channel backfills existing rows to
-- 'unknown' (there is no way to know, retroactively, which transport
-- created a pre-migration row -- gRPC already existed as of Fase 6) then
-- drops its default so every INSERT going forward must supply one
-- explicitly, same pattern as migration 00005's `scope` column.
--
-- trace_id is the request-level correlation identifier captured from the
-- CURRENT span at creation time (nil/omitted when otel_enabled=false or no
-- valid span exists) -- a payment row surviving after the trace exporter's
-- data ages out stays traceable to its originating request.
--
-- description is a free-text merchant reference/memo (e.g. "invoice #1234")
-- -- common in real PJP APIs, flows through to statements/receipts.
--
-- initiated_by distinguishes customer-initiated vs system-initiated (e.g.
-- automated reconciliation) vs admin-initiated (e.g. a future manual retry
-- endpoint) actors. At the AGGREGATE level (this section) it describes who
-- initiated the original create request -- always 'customer' today (the
-- only creation path that exists), backfilled and defaulted accordingly.
--
-- request_ip / request_user_agent are captured from the transport layer at
-- creation time (fiber.Ctx for REST; best-effort peer/metadata for gRPC) --
-- nullable, since not every future transport can supply them.
ALTER TABLE payments ADD COLUMN source_app text;
ALTER TABLE payments ADD COLUMN channel text NOT NULL DEFAULT 'unknown';
ALTER TABLE payments ALTER COLUMN channel DROP DEFAULT;
ALTER TABLE payments ADD COLUMN trace_id text;
ALTER TABLE payments ADD COLUMN description text;
ALTER TABLE payments ADD COLUMN initiated_by text NOT NULL DEFAULT 'customer'
    CHECK (initiated_by IN ('customer', 'system', 'admin'));
ALTER TABLE payments ALTER COLUMN initiated_by DROP DEFAULT;
ALTER TABLE payments ADD COLUMN request_ip text;
ALTER TABLE payments ADD COLUMN request_user_agent text;

ALTER TABLE refunds ADD COLUMN source_app text;
ALTER TABLE refunds ADD COLUMN channel text NOT NULL DEFAULT 'unknown';
ALTER TABLE refunds ALTER COLUMN channel DROP DEFAULT;
ALTER TABLE refunds ADD COLUMN trace_id text;
ALTER TABLE refunds ADD COLUMN description text;
ALTER TABLE refunds ADD COLUMN initiated_by text NOT NULL DEFAULT 'customer'
    CHECK (initiated_by IN ('customer', 'system', 'admin'));
ALTER TABLE refunds ALTER COLUMN initiated_by DROP DEFAULT;
ALTER TABLE refunds ADD COLUMN request_ip text;
ALTER TABLE refunds ADD COLUMN request_user_agent text;
-- Consistency fix: refunds never got an operation_type column, even though
-- the task's money-invariant list requires it present "on every operation
-- type consistently" -- payments and (as of migration 00008) payouts both
-- have one. Fixed value, same pattern as payouts.operation_type.
ALTER TABLE refunds ADD COLUMN operation_type text NOT NULL DEFAULT 'refund'
    CHECK (operation_type = 'refund');

ALTER TABLE payouts ADD COLUMN source_app text;
ALTER TABLE payouts ADD COLUMN channel text NOT NULL DEFAULT 'unknown';
ALTER TABLE payouts ALTER COLUMN channel DROP DEFAULT;
ALTER TABLE payouts ADD COLUMN trace_id text;
ALTER TABLE payouts ADD COLUMN description text;
ALTER TABLE payouts ADD COLUMN initiated_by text NOT NULL DEFAULT 'customer'
    CHECK (initiated_by IN ('customer', 'system', 'admin'));
ALTER TABLE payouts ALTER COLUMN initiated_by DROP DEFAULT;
ALTER TABLE payouts ADD COLUMN request_ip text;
ALTER TABLE payouts ADD COLUMN request_user_agent text;

-- Event-level initiated_by: per-TRANSITION actor, distinct from the
-- aggregate-level column above -- a payment's row-level initiated_by says
-- who created it (always 'customer' today); each event row says what kind
-- of process drove THAT transition (worker/ProcessCharge, a webhook
-- callback, or the reconciliation job -- all 'system' today; 'admin'
-- reserved for a future manual-retry endpoint, same enum-member-with-no-
-- current-caller precedent as operation_type's 'authorize'/'capture'/
-- 'reversal'). This is what makes reconciliation-applied transitions
-- queryable/distinguishable from other transitions, not just guessable from
-- event_type string prefixes.
ALTER TABLE payment_events ADD COLUMN initiated_by text NOT NULL DEFAULT 'system'
    CHECK (initiated_by IN ('customer', 'system', 'admin'));
ALTER TABLE payment_events ALTER COLUMN initiated_by DROP DEFAULT;

ALTER TABLE refund_events ADD COLUMN initiated_by text NOT NULL DEFAULT 'system'
    CHECK (initiated_by IN ('customer', 'system', 'admin'));
ALTER TABLE refund_events ALTER COLUMN initiated_by DROP DEFAULT;

ALTER TABLE payout_events ADD COLUMN initiated_by text NOT NULL DEFAULT 'system'
    CHECK (initiated_by IN ('customer', 'system', 'admin'));
ALTER TABLE payout_events ALTER COLUMN initiated_by DROP DEFAULT;

-- +goose Down

ALTER TABLE payout_events DROP COLUMN IF EXISTS initiated_by;
ALTER TABLE refund_events DROP COLUMN IF EXISTS initiated_by;
ALTER TABLE payment_events DROP COLUMN IF EXISTS initiated_by;

ALTER TABLE payouts DROP COLUMN IF EXISTS request_user_agent;
ALTER TABLE payouts DROP COLUMN IF EXISTS request_ip;
ALTER TABLE payouts DROP COLUMN IF EXISTS initiated_by;
ALTER TABLE payouts DROP COLUMN IF EXISTS description;
ALTER TABLE payouts DROP COLUMN IF EXISTS trace_id;
ALTER TABLE payouts DROP COLUMN IF EXISTS channel;
ALTER TABLE payouts DROP COLUMN IF EXISTS source_app;

ALTER TABLE refunds DROP COLUMN IF EXISTS operation_type;
ALTER TABLE refunds DROP COLUMN IF EXISTS request_user_agent;
ALTER TABLE refunds DROP COLUMN IF EXISTS request_ip;
ALTER TABLE refunds DROP COLUMN IF EXISTS initiated_by;
ALTER TABLE refunds DROP COLUMN IF EXISTS description;
ALTER TABLE refunds DROP COLUMN IF EXISTS trace_id;
ALTER TABLE refunds DROP COLUMN IF EXISTS channel;
ALTER TABLE refunds DROP COLUMN IF EXISTS source_app;

ALTER TABLE payments DROP COLUMN IF EXISTS request_user_agent;
ALTER TABLE payments DROP COLUMN IF EXISTS request_ip;
ALTER TABLE payments DROP COLUMN IF EXISTS initiated_by;
ALTER TABLE payments DROP COLUMN IF EXISTS description;
ALTER TABLE payments DROP COLUMN IF EXISTS trace_id;
ALTER TABLE payments DROP COLUMN IF EXISTS channel;
ALTER TABLE payments DROP COLUMN IF EXISTS source_app;
