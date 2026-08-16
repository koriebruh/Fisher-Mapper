-- +goose Up

-- Seeds for the queue-name and OTel-toggle dynamic config keys (mirrors
-- configs/config.toml's [queue].default_name / [observability].otel_enabled
-- -- keep those two files' literals in sync by hand, same convention as
-- provider.mock.enabled in 00004_app_config.sql). ON CONFLICT DO NOTHING
-- unlike 00004's seed: by Fase 4 this table may already have operator-set
-- rows from a prior deploy, and a migration must never clobber those.
INSERT INTO app_config (key, value, updated_by)
VALUES ('queue.default_name', 'payments', 'migration_seed')
ON CONFLICT (key) DO NOTHING;

INSERT INTO app_config (key, value, updated_by)
VALUES ('observability.otel_enabled', 'true', 'migration_seed')
ON CONFLICT (key) DO NOTHING;

-- +goose Down

DELETE FROM app_config WHERE key IN ('queue.default_name', 'observability.otel_enabled');
