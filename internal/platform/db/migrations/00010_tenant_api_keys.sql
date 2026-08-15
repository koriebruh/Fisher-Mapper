-- +goose Up

-- CRITICAL security fix: closes the "no caller authentication anywhere"
-- finding -- tenant_id was previously self-asserted in the request body
-- (write side) and every Get* read was unscoped by tenant entirely. This
-- table backs the REST/gRPC tenant-authentication middleware
-- (internal/platform/tenantauth.Store), mirroring the existing single
-- static admin API key (rest/admin.go's adminAuth) but per-tenant: one row
-- per issued key, api_key -> tenant_id.
--
-- Deliberately NO seed rows here (unlike migration 00006/00007's dynamic
-- config seeds): a seed row would mean a working, source-visible API key
-- landing in every environment that runs `make migrate-up`, including
-- production -- the exact anti-pattern cmd/server/main.go's adminAPIKey
-- wiring explicitly rejects ("Deliberately NO hardcoded fallback when
-- unset ... falling open to a fixed, source-visible literal ... would be a
-- real auth bypass"). Keys are issued instead via `cmd/migrate
-- -create-tenant-key <tenant_id>`, which generates a random key with
-- crypto/rand, inserts it, and prints it once to stdout -- nothing
-- resembling a credential ever enters git history.
--
-- api_key is looked up on every authenticated request (REST middleware +
-- gRPC interceptor), so it needs an index; UNIQUE already gives it one.
-- tenant_id has its own index since an operator listing/revoking a given
-- tenant's keys is a real operation this table must support cheaply.
CREATE TABLE tenant_api_keys (
    id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    api_key    text NOT NULL UNIQUE,
    tenant_id  text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_tenant_api_keys_tenant_id ON tenant_api_keys (tenant_id);

-- +goose Down

DROP TABLE IF EXISTS tenant_api_keys;
