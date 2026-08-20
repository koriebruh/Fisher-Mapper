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

// downFlag/createTenantKeyFlag wrap the CLI flags as distinct types so dig
// never confuses them with any other provided string/bool.
type downFlag bool
type createTenantKeyFlag string

func provideLogger() *slog.Logger {
	logger := observability.NewLogger("info")
	slog.SetDefault(logger)
	return logger
}

func provideConfig() (config.Bootstrap, error) {
	config.LoadDotEnv()
	return config.Load(configPath())
}

func providePool(cfg config.Bootstrap, lc fx.Lifecycle) (*pgxpool.Pool, error) {
	pool, err := db.NewPool(context.Background(), cfg.Postgres.DSN)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	lc.Append(fx.Hook{
		OnStop: func(context.Context) error {
			pool.Close()
			return nil
		},
	})
	return pool, nil
}

// runMigrate is the sole fx.Invoke: this is a one-shot CLI, not a
// long-running server, so there is nothing to wait for a signal on. It
// registers an OnStart hook that does the real work, then always calls
// Shutdowner -- app.Run() (see main.go) blocks until that call, runs OnStop
// (pool.Close), then exits with the given code. This is fx's documented
// pattern for short-lived applications.
func runMigrate(lc fx.Lifecycle, sh fx.Shutdowner, logger *slog.Logger, pool *pgxpool.Pool, down downFlag, createTenantKey createTenantKeyFlag) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			exitCode := 0
			if err := doMigrate(ctx, logger, pool, bool(down), string(createTenantKey)); err != nil {
				logger.Error(err.Error())
				exitCode = 1
			}
			return sh.Shutdown(fx.ExitCode(exitCode))
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
