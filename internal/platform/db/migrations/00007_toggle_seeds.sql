-- +goose Up

-- Seeds for the ratelimit/circuitbreaker/reconciliation on/off toggles
-- (mirrors config.toml's [ratelimit]/[circuitbreaker]/[reconciliation]
-- .enabled -- keep those literals in sync by hand, same convention as
-- 00006_dynamic_config_seeds.sql). ON CONFLICT DO NOTHING: this table may
-- already have operator-set rows from a prior deploy by the time this
-- migration runs, and a migration must never clobber those.
INSERT INTO app_config (key, value, updated_by)
VALUES ('ratelimit.enabled', 'true', 'migration_seed')
ON CONFLICT (key) DO NOTHING;

INSERT INTO app_config (key, value, updated_by)
VALUES ('circuitbreaker.enabled', 'true', 'migration_seed')
ON CONFLICT (key) DO NOTHING;

INSERT INTO app_config (key, value, updated_by)
VALUES ('reconciliation.enabled', 'true', 'migration_seed')
ON CONFLICT (key) DO NOTHING;

-- +goose Down

DELETE FROM app_config WHERE key IN ('ratelimit.enabled', 'circuitbreaker.enabled', 'reconciliation.enabled');
