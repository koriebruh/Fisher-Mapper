-- +goose Up

-- HIGH finding fix: ApplyProviderEvent/SweepStagedWebhooks could only ever
-- resolve incoming_webhook_events against the payments table --
-- payment_id's FK is REFERENCES payments (id), so a webhook that actually
-- belongs to a refund or a payout (keyed by provider_refund_ref / the
-- payout's own provider_ref) had no way to be marked processed against the
-- row it really resolved to, and sat staged forever. Add nullable
-- refund_id/payout_id columns, mirroring payment_id's shape exactly (each
-- FK's target row is only known once resolved -- these are set at most one
-- of the three, never more than one, since a single staged event resolves
-- to exactly one entity).
ALTER TABLE incoming_webhook_events ADD COLUMN refund_id uuid REFERENCES refunds (id);
ALTER TABLE incoming_webhook_events ADD COLUMN payout_id uuid REFERENCES payouts (id);

CREATE INDEX incoming_webhook_events_refund_id_idx ON incoming_webhook_events (refund_id);
CREATE INDEX incoming_webhook_events_payout_id_idx ON incoming_webhook_events (payout_id);

-- +goose Down

DROP INDEX IF EXISTS incoming_webhook_events_payout_id_idx;
DROP INDEX IF EXISTS incoming_webhook_events_refund_id_idx;
ALTER TABLE incoming_webhook_events DROP COLUMN IF EXISTS payout_id;
ALTER TABLE incoming_webhook_events DROP COLUMN IF EXISTS refund_id;
