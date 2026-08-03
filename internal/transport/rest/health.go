// Package rest holds the REST (fiber) transport. Phase 1 only wires the
// health-check surface; payment endpoints arrive in later phases.
package rest

import (
	"context"
	"log/slog"

	"github.com/gofiber/fiber/v2"
	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgxpool"

	"Fisher-Mapper/internal/domain/payment"
	"Fisher-Mapper/internal/queue"
)

// Deps are the dependencies the REST transport needs across all route
// groups. Phase 1 only used Pool/QueueClient (health checks); Phase 2 adds
// PaymentService for the create-payment endpoint. Kept as one struct since
// main() wires everything at once into a single fiber.App.
type Deps struct {
	Pool           *pgxpool.Pool
	QueueClient    *asynq.Client
	PaymentService *payment.Service
}

// NewApp builds the fiber app and registers all routes.
func NewApp(deps Deps) *fiber.App {
	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})

	RegisterHealthRoutes(app, deps)
	if deps.PaymentService != nil {
		RegisterPaymentRoutes(app, PaymentDeps{Service: deps.PaymentService})
	}

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
			slog.Warn("readyz: postgres not ready", "error", err)
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"status": "not ready",
				"reason": "postgres",
			})
		}

		if err := checkRedis(deps.QueueClient); err != nil {
			slog.Warn("readyz: redis not ready", "error", err)
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"status": "not ready",
				"reason": "redis",
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
