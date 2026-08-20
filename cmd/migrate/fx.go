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
// never confuses them with any other supplied string/bool.
type downFlag bool
type createTenantKeyFlag string

func newLogger() *slog.Logger {
	logger := observability.NewLogger("info")
	slog.SetDefault(logger)
	return logger
}

// loadConfig and connectPool run as plain function calls in main(), BEFORE
// fx.New -- not as fx.Provide constructors. Both are one-shot,
// failure-prone bootstrap steps; routing them through dig's dependency
// resolution would prefix their error messages with dig's own
// "could not build arguments for function ..." wrapper text, which is a
// real change to what gets logged on a bad DSN or malformed config.toml (a
// scenario this CLI is explicitly expected to fail loudly and cleanly on).
// Calling them directly preserves the exact original single-line error
// messages. fx still owns everything downstream: pool cleanup via
// fx.Lifecycle (registerPoolCleanup) and process exit via fx.Shutdowner
// (runMigrate).
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

// runMigrate is the sole fx.Invoke that does real work: this is a one-shot
// CLI, not a long-running server, so there is nothing to wait for a signal
// on. It registers an OnStart hook that does the migrate/rollback/
// create-tenant-key work, then always calls Shutdowner -- app.Run() (see
// main.go) blocks until that call, runs OnStop (pool cleanup), then exits
// with the given code. This is fx's documented pattern for short-lived
// applications.
//
// The OnStart hook deliberately uses context.Background() for the actual
// work, NOT the ctx fx passes in: fx.App wraps Start() in its own internal
// StartTimeout (15s by default, see fx.DefaultTimeout), and a real
// migration (a large ALTER TABLE, an index build, a backfill) can validly
// take longer than that. The original pre-fx code had no such ceiling
// (context.Background() throughout); using fx's ctx here would silently
// impose one and cancel a legitimate long-running migration mid-flight,
// skipping OnStop (pool cleanup) entirely when it fired.
func runMigrate(lc fx.Lifecycle, sh fx.Shutdowner, logger *slog.Logger, pool *pgxpool.Pool, down downFlag, createTenantKey createTenantKeyFlag) {
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			exitCode := 0
			if err := doMigrate(context.Background(), logger, pool, bool(down), string(createTenantKey)); err != nil {
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
