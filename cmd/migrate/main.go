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
	"log/slog"
	"os"

	"Fisher-Mapper/internal/config"
	"Fisher-Mapper/internal/db"
	"Fisher-Mapper/internal/observability"
)

const migrationsDir = "internal/db/migrations"

func main() {
	down := flag.Bool("down", false, "roll back the single most recent migration instead of applying pending ones")
	flag.Parse()

	logger := observability.NewLogger("info")
	slog.SetDefault(logger)

	// Load .env (repo root), if present, BEFORE bootstrap config below --
	// its values must be in the process environment before config.Load's
	// env var overlay step runs.
	config.LoadDotEnv()

	ctx := context.Background()

	cfg, err := config.Load(configPath())
	if err != nil {
		logger.Error("load bootstrap config", "error", err)
		os.Exit(1)
	}

	pool, err := db.NewPool(ctx, cfg.Postgres.DSN)
	if err != nil {
		logger.Error("connect postgres", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	sqlDB := db.NewMigrationHandle(pool)
	defer sqlDB.Close()

	if *down {
		if err := db.RollbackLastMigration(ctx, sqlDB, migrationsDir); err != nil {
			logger.Error("rollback migration", "error", err)
			os.Exit(1)
		}
		logger.Info("last migration rolled back")
		return
	}

	if err := db.RunMigrations(ctx, sqlDB, migrationsDir); err != nil {
		logger.Error("run migrations", "error", err)
		os.Exit(1)
	}

	logger.Info("migrations applied")
}

func configPath() string {
	if v := os.Getenv("APP_CONFIG_FILE"); v != "" {
		return v
	}
	return "config.toml"
}
