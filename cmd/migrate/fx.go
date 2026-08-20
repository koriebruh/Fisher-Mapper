package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"Fisher-Mapper/internal/platform/config"
	"Fisher-Mapper/internal/platform/db"
	"Fisher-Mapper/internal/platform/observability"
	"Fisher-Mapper/internal/platform/tenantauth"
)

const migrationsDir = "internal/platform/db/migrations"

func newLogger() *slog.Logger {
	logger := observability.NewLogger("info")
	slog.SetDefault(logger)
	return logger
}

// loadConfig and connectPool run as plain function calls in main(), BEFORE
// fx.New -- not as fx.Provide constructors. Both are one-shot,
// failure-prone bootstrap steps; routing them through dig's dependency
// resolution would prefix their error messages with dig's own
// "could not build arguments for function ..." wrapper text instead of the
// original clean single-line message. fx's role in this binary is
// deliberately narrow: pool cleanup ordering via fx.Lifecycle
// (registerPoolCleanup) -- the actual migrate/rollback/create-tenant-key
// work runs as a plain call in main(), between app.Start() and app.Stop(),
// for the same reason: fx.App bounds OnStart/OnStop hooks with its own
// internal StartTimeout/StopTimeout (15s default each), and a real
// migration can validly run far longer than that. There is no fx.Invoke
// (or fx.Lifecycle.OnStart) doing real work anywhere in this file.
func loadConfig() (config.Bootstrap, error) {
	config.LoadDotEnv()
	return config.Load(configPath())
}

func connectPool(cfg config.Bootstrap) (*pgxpool.Pool, error) {
	return db.NewPool(context.Background(), cfg.Postgres.DSN)
}

func registerPoolCleanup(lc fx.Lifecycle, pool *pgxpool.Pool) {
	lc.Append(fx.Hook{
		OnStop: func(context.Context) error {
			pool.Close()
			return nil
		},
	})
}

func doMigrate(ctx context.Context, logger *slog.Logger, pool *pgxpool.Pool, down bool, createTenantKey string) error {
	if createTenantKey != "" {
		apiKey, err := tenantauth.NewStore(pool).CreateKey(ctx, createTenantKey)
		if err != nil {
			return fmt.Errorf("create tenant key: %w", err)
		}
		// codeql[go/clear-text-logging] -- one-time operator-facing credential
		// display, no other retrieval channel; see prior main.go history for
		// the accepted-risk dismissal this carries forward unchanged.
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
