-- +goose Up

-- payment_method was accepted on the create-payment request (REST/gRPC) and
-- flowed all the way into provider.ChargeRequest, but was never persisted --
-- a caller had no way to see back which method (qris/virtual_account/card/
-- ...) a charge actually used. Unlike channel (migration 00009), the default
-- is NOT dropped: payment_method is a genuinely optional business field (a
-- provider can be method-agnostic), not a "who/how" fact every row must
-- carry, so existing direct-SQL test fixtures that never mention this column
-- keep working.
ALTER TABLE payments ADD COLUMN payment_method text NOT NULL DEFAULT '';

-- method_payload carries the method-specific response data a caller needs to
-- actually complete the payment (a QRIS string to render, a VA number to
-- display, a card redirect URL, ...) -- see internal/provider/payload.
-- Nullable: most operation types (and methods with no such payload) never
-- populate it.
ALTER TABLE payments ADD COLUMN method_payload jsonb;

-- callback_url is a caller-supplied best-effort delivery target notified
-- once a charge/payout reaches a terminal state (see
-- Service.WithCallbackNotifier). Nullable: entirely optional, most callers
-- poll GetPayment/GetPayout instead.
ALTER TABLE payments ADD COLUMN callback_url text;
ALTER TABLE payouts ADD COLUMN callback_url text;

-- +goose Down

ALTER TABLE payouts DROP COLUMN IF EXISTS callback_url;
ALTER TABLE payments DROP COLUMN IF EXISTS callback_url;
ALTER TABLE payments DROP COLUMN IF EXISTS method_payload;
ALTER TABLE payments DROP COLUMN IF EXISTS payment_method;
