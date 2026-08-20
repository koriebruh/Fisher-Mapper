// Command migrate applies pending goose migrations against the Postgres
// instance described by bootstrap config (config.toml at the repo root +
// env overrides), using the same config loader and connection helpers as
// cmd/server so there is exactly one source of truth for the DSN.
//
// It is intentionally a separate binary from cmd/server: schema migration
// is a deliberate operator action, not something the request-serving
// process should trigger implicitly on every boot.
//
// Config loading and pool construction happen as plain function calls
// below, before fx.New -- see loadConfig/connectPool's doc in fx.go for
// why. Everything downstream (pool cleanup, process exit) is wired as an
// fx dependency graph -- see fx.go. This is a one-shot CLI, so it uses
// fx.Shutdowner from inside an OnStart hook to end the process rather than
// fx.App.Run()'s normal wait-for-SIGINT/SIGTERM behavior.
package main

import (
	"flag"
	"fmt"
	"os"

	"go.uber.org/fx"
)

func main() {
	down := flag.Bool("down", false, "roll back the single most recent migration instead of applying pending ones")
	createTenantKey := flag.String("create-tenant-key", "", "generate a tenant_api_keys row for this tenant_id, print the new key to stdout, then exit (no migrations run)")
	flag.Parse()

	logger := newLogger()

	cfg, err := loadConfig()
	if err != nil {
		logger.Error(fmt.Errorf("load bootstrap config: %w", err).Error())
		os.Exit(1)
	}

	pool, err := connectPool(cfg)
	if err != nil {
		logger.Error(fmt.Errorf("connect postgres: %w", err).Error())
		os.Exit(1)
	}

	app := fx.New(
		fx.Supply(logger, pool, downFlag(*down), createTenantKeyFlag(*createTenantKey)),
		fx.Invoke(registerPoolCleanup, runMigrate),
		fx.NopLogger,
	)

	if err := app.Err(); err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	app.Run()
}
