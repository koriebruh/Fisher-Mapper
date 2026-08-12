-- +goose Up

-- app_config is the Fase 4 dynamic-config table (plan "Prinsip Arsitektur
-- Dasar" item 2): loaded AFTER the bootstrap Postgres connection exists,
-- cached in memory by internal/config.Cache with periodic refresh. Values
-- are plain text (interpreted by the reader -- bool/int/string) rather than
-- typed columns, since the whole point of this table is that new flags can
-- be added without a migration.
CREATE TABLE app_config (
    key         text PRIMARY KEY,
    value       text NOT NULL,
    updated_by  text NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- Seed the mock provider as enabled by default so a fresh docker-compose
-- stack behaves exactly like Fase 3 (provider on) until an operator
-- explicitly flips it -- config.toml's role here is "seed defaults for this
-- table", per the plan, and this INSERT is the seed.
INSERT INTO app_config (key, value, updated_by)
VALUES ('provider.mock.enabled', 'true', 'migration_seed');

-- app_config_audit is the Fase 4 "stub cheap" admin audit table (plan:
-- "Admin config audit table (app_config_audit): siapa ubah apa kapan").
-- Written in the SAME transaction as every app_config UPDATE/INSERT (see
-- internal/config.DynamicStore.SetWithAudit) so a config change without a
-- matching audit row is impossible by construction, not by convention.
CREATE TABLE app_config_audit (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    config_key  text NOT NULL,
    old_value   text,
    new_value   text NOT NULL,
    changed_by  text NOT NULL,
    changed_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX app_config_audit_config_key_idx ON app_config_audit (config_key);

-- +goose Down

DROP TABLE IF EXISTS app_config_audit;
DROP TABLE IF EXISTS app_config;
