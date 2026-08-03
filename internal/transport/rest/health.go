// Package rest holds the REST (fiber) transport. Phase 1 only wires the
// health-check surface; payment endpoints arrive in later phases.
package rest

import (
	"context"

	"github.com/gofiber/fiber/v2"
	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgxpool"

	"Fisher-Mapper/internal/queue"
)

// Deps are the dependencies health checks need to report readiness.
type Deps struct {
	Pool        *pgxpool.Pool
	QueueClient *asynq.Client
}

// NewApp builds the fiber app and registers the Phase 1 routes.
func NewApp(deps Deps) *fiber.App {
	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})

	RegisterHealthRoutes(app, deps)

	return app
}

// RegisterHealthRoutes registers /healthz and /readyz.
//
// /healthz reports process liveness only (no dependency checks) — it
// answers "is the process alive and able to handle a request", the
// standard k8s liveness-probe contract.
//
// /readyz reports whether the process is ready to serve traffic: Postgres
// and Redis must both be reachable.
func RegisterHealthRoutes(app *fiber.App, deps Deps) {
	app.Get("/healthz", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"status": "ok"})
	})

	app.Get("/readyz", func(c *fiber.Ctx) error {
		ctx := c.Context()

		if err := checkPostgres(ctx, deps.Pool); err != nil {
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"status": "not ready",
				"reason": "postgres: " + err.Error(),
			})
		}

		if err := checkRedis(deps.QueueClient); err != nil {
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"status": "not ready",
				"reason": "redis: " + err.Error(),
			})
		}

		return c.JSON(fiber.Map{"status": "ok"})
	})
}

func checkPostgres(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		return fiber.NewError(fiber.StatusServiceUnavailable, "postgres pool not configured")
	}
	return pool.Ping(ctx)
}

func checkRedis(client *asynq.Client) error {
	if client == nil {
		return fiber.NewError(fiber.StatusServiceUnavailable, "queue client not configured")
	}
	return queue.Ping(client)
}
