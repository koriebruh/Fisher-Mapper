// Command migrate applies pending goose migrations against the Postgres
// instance described by bootstrap config (config.toml at the repo root +
// env overrides), using the same config loader and connection helpers as
// cmd/server so there is exactly one source of truth for the DSN.
//
// It is intentionally a separate binary from cmd/server: schema migration
// is a deliberate operator action, not something the request-serving
// process should trigger implicitly on every boot.
//
// Wiring (config -> pool -> migrate-or-rollback-or-create-key) is built as
// an fx dependency graph -- see fx.go. This is a one-shot CLI, so it uses
// fx.Shutdowner from inside an OnStart hook to end the process rather than
// fx.App.Run()'s normal wait-for-SIGINT/SIGTERM behavior.
package main

import (
	"flag"
	"log/slog"
	"os"

	"go.uber.org/fx"
)

func main() {
	down := flag.Bool("down", false, "roll back the single most recent migration instead of applying pending ones")
	createTenantKey := flag.String("create-tenant-key", "", "generate a tenant_api_keys row for this tenant_id, print the new key to stdout, then exit (no migrations run)")
	flag.Parse()

	app := fx.New(
		fx.Supply(downFlag(*down), createTenantKeyFlag(*createTenantKey)),
		fx.Provide(provideLogger, provideConfig, providePool),
		fx.Invoke(runMigrate),
		fx.NopLogger,
	)

	if err := app.Err(); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
	app.Run()
}
