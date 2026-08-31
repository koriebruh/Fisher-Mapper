// Command migrate applies pending goose migrations against the Postgres
// instance described by bootstrap config (config.toml at the repo root +
// env overrides), using the same config loader and connection helpers as
// cmd/server so there is exactly one source of truth for the DSN.
//
// It is intentionally a separate binary from cmd/server: schema migration
// is a deliberate operator action, not something the request-serving
// process should trigger implicitly on every boot.
//
// fx's role here is deliberately narrow -- see fx.go's doc on
// loadConfig/connectPool for why config/pool construction and the actual
// migration work are plain function calls rather than fx.Provide/fx.Invoke:
// fx.App's own internal Start/Stop timeouts (15s default) would otherwise
// silently bound a migration's runtime and its error messages. fx is used
// only for pool cleanup ordering (fx.Lifecycle, via registerPoolCleanup),
// driven manually with app.Start()/app.Stop() rather than app.Run() (which
// waits for a SIGINT/SIGTERM this one-shot CLI never receives).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"go.uber.org/fx"
)

func main() {
	down := flag.Bool("down", false, "roll back the single most recent migration instead of applying pending ones")
	createTenantKey := flag.String("create-tenant-key", "", "generate a tenant_api_keys row for this tenant_id, print the new key to stdout, then exit (no migrations run)")
	flag.Parse()

	// run() owns every defer (the Start/Stop timeout contexts' cancel funcs)
	// so os.Exit -- which skips deferred calls -- only ever happens here,
	// after they've already run.
	os.Exit(run(*down, *createTenantKey))
}

func run(down bool, createTenantKey string) int {
	logger := newLogger()

	cfg, err := loadConfig()
	if err != nil {
		logger.Error(fmt.Errorf("load bootstrap config: %w", err).Error())
		return 1
	}

	pool, err := connectPool(cfg)
	if err != nil {
		logger.Error(fmt.Errorf("connect postgres: %w", err).Error())
		return 1
	}

	app := fx.New(
		fx.Supply(pool),
		fx.Invoke(registerPoolCleanup),
		fx.NopLogger,
	)
	if err := app.Err(); err != nil {
		logger.Error(err.Error())
		return 1
	}

	startCtx, cancelStart := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelStart()
	if err := app.Start(startCtx); err != nil {
		logger.Error(err.Error())
		return 1
	}

	exitCode := 0
	if err := doMigrate(context.Background(), logger, pool, down, createTenantKey); err != nil {
		logger.Error(err.Error())
		exitCode = 1
	}

	stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelStop()
	if err := app.Stop(stopCtx); err != nil {
		logger.Warn("shutdown fx app", "error", err)
	}

	return exitCode
}
