-- +goose Up

-- outbox backs internal/outbox: the domain write (payment row insert) and
-- this row are inserted in the SAME Postgres transaction (see
-- payment.Repository.CreateWithOutbox), so "payment created" and "task
-- exists to process it" can never diverge -- if the transaction never
-- commits, neither exists; if it commits, both do.
--
-- The relay (internal/outbox.Relay) polls WHERE status = 'pending' FOR
-- UPDATE SKIP LOCKED, dispatches to the queue client, and marks the row
-- 'dispatched' in the same transaction as the claim. Per the plan's Fase 3
-- "Catatan desain wajib": retrying a *dispatch* (re-running this poll loop,
-- re-enqueueing) is safe/idempotent -- it is NOT the thing that calls the
-- provider. The provider call happens once, in the worker, guarded by the
-- payment state machine's pending->processing compare-and-swap, not by
-- anything in this table.
CREATE TABLE outbox (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    task_type       text NOT NULL,
    payload         jsonb NOT NULL,
    status          text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'dispatched')),
    attempts        int NOT NULL DEFAULT 0,
    last_error      text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    dispatched_at   timestamptz
);

-- Polling predicate is always "status = 'pending' ORDER BY created_at" --
-- a plain b-tree on (status, created_at) is what FOR UPDATE SKIP LOCKED
-- scans.
CREATE INDEX outbox_status_created_at_idx ON outbox (status, created_at);

-- terminal_failures is the DLQ inspection table (plan: "asynq archive queue
-- (built-in) + tabel terminal_failures di-populate via error handler
-- callback asynq"). Populated only for genuinely unexpected task failures
-- (unregistered provider, malformed payload, DB write failure) -- NEVER for
-- an expected "provider call timed out, payment stays processing" outcome,
-- which the charge task handler reports as success (nil error) precisely so
-- it does not show up here or in asynq's archive.
CREATE TABLE terminal_failures (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    task_type   text NOT NULL,
    task_id     text,
    payload     jsonb,
    error       text NOT NULL,
    failed_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX terminal_failures_task_type_idx ON terminal_failures (task_type);

-- +goose Down

DROP TABLE IF EXISTS terminal_failures;
DROP TABLE IF EXISTS outbox;
