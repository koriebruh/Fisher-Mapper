// Command migrate applies pending goose migrations against the Postgres
// instance described by bootstrap config (config.toml at the repo root +
// env overrides), using the same config loader and connection helpers as
// cmd/server so there is exactly one source of truth for the DSN.
//
// It is intentionally a separate binary from cmd/server: schema migration
// is a deliberate operator action, not something the request-serving
// process should trigger implicitly on every boot.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"Fisher-Mapper/internal/platform/config"
	"Fisher-Mapper/internal/platform/db"
	"Fisher-Mapper/internal/platform/observability"
	"Fisher-Mapper/internal/platform/tenantauth"
)

const migrationsDir = "internal/platform/db/migrations"

func main() {
	down := flag.Bool("down", false, "roll back the single most recent migration instead of applying pending ones")
	createTenantKey := flag.String("create-tenant-key", "", "generate a tenant_api_keys row for this tenant_id, print the new key to stdout, then exit (no migrations run)")
	flag.Parse()

	logger := observability.NewLogger("info")
	slog.SetDefault(logger)

	// Load .env (repo root), if present, BEFORE bootstrap config below --
	// its values must be in the process environment before config.Load's
	// env var overlay step runs.
	config.LoadDotEnv()

	// run() owns every defer (pool.Close, sqlDB.Close) so os.Exit -- which
	// skips deferred calls -- only ever happens here, after they've already
	// run.
	if err := run(context.Background(), logger, *down, *createTenantKey); err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger, down bool, createTenantKey string) error {
	cfg, err := config.Load(configPath())
	if err != nil {
		return fmt.Errorf("load bootstrap config: %w", err)
	}

	pool, err := db.NewPool(ctx, cfg.Postgres.DSN)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()

	// -create-tenant-key is a one-off credential-issuing operation, not a
	// migration -- checked before touching goose/sqlDB at all so it works
	// against a pool whether or not migration 00010 has been the last one
	// applied yet (it does need that table to exist, but this flag's whole
	// point is being invoked as its own separate operator step, same as
	// -down).
	if createTenantKey != "" {
		apiKey, err := tenantauth.NewStore(pool).CreateKey(ctx, createTenantKey)
		if err != nil {
			return fmt.Errorf("create tenant key: %w", err)
		}
		fmt.Println(apiKey)
		return nil
	}

	sqlDB := db.NewMigrationHandle(pool)
	defer func() {
		if cerr := sqlDB.Close(); cerr != nil {
			logger.Warn("close migration handle", "error", cerr)
		}
	}()

	if down {
		if err := db.RollbackLastMigration(ctx, sqlDB, migrationsDir); err != nil {
			return fmt.Errorf("rollback migration: %w", err)
		}
		logger.Info("last migration rolled back")
		return nil
	}

	if err := db.RunMigrations(ctx, sqlDB, migrationsDir); err != nil {
		return fmt.Errorf("run migrations: %w", err)
	}

	logger.Info("migrations applied")
	return nil
}

func configPath() string {
	if v := os.Getenv("APP_CONFIG_FILE"); v != "" {
		return v
	}
	return "config.toml"
}
