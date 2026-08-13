package db

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

// NewMigrationHandle adapts an existing pgxpool.Pool into a *sql.DB for
// goose, via stdlib.OpenDBFromPool.
//
// Deliberately NOT using sql.Open("pgx", dsn): that path relies on the pgx
// driver having been registered as a side effect of importing
// github.com/jackc/pgx/v5/stdlib (its init() calls sql.Register). Per the
// project's "no blank import for order-sensitive registration" rule, we
// avoid depending on that implicit registration entirely and instead go
// through the explicit, exported OpenDBFromPool constructor, reusing the
// pool created by NewPool. The caller (bootstrap/register.go) is
// responsible for calling this at the right point in startup order and for
// closing the returned *sql.DB (it does not close the underlying pool).
func NewMigrationHandle(pool *pgxpool.Pool) *sql.DB {
	return stdlib.OpenDBFromPool(pool)
}

// RunMigrations applies all pending goose migrations found in dir against
// db. SetDialect is called explicitly here (not via package init) each time,
// which is cheap and keeps the dialect selection visible at the call site.
func RunMigrations(ctx context.Context, sqlDB *sql.DB, dir string) error {
	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("db: set goose dialect: %w", err)
	}
	if err := goose.UpContext(ctx, sqlDB, dir); err != nil {
		return fmt.Errorf("db: run migrations: %w", err)
	}
	return nil
}

// RollbackLastMigration reverts the single most recently applied goose
// migration found in dir. Used by `make migrate-down` / `cmd/migrate -down`
// for local development; the goose CLI binary is not used here because
// building it pulls in optional driver dependencies (clickhouse, mysql,
// mssql, ...) that are not in this module's go.sum.
func RollbackLastMigration(ctx context.Context, sqlDB *sql.DB, dir string) error {
	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("db: set goose dialect: %w", err)
	}
	if err := goose.DownContext(ctx, sqlDB, dir); err != nil {
		return fmt.Errorf("db: rollback migration: %w", err)
	}
	return nil
}
