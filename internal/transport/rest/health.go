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
	"Fisher-Mapper/internal/platform/config"
	"Fisher-Mapper/internal/platform/queue"
	"Fisher-Mapper/internal/provider"
	"Fisher-Mapper/internal/provider/auth"
	"Fisher-Mapper/internal/ratelimit"
	"Fisher-Mapper/internal/webhook"
)

// Deps are the dependencies the REST transport needs across all route
// groups. Kept as one struct since main() wires everything at once into a
// single fiber.App. Providers/Verifiers/WebhookStore are Fase 3 additions
// backing POST /webhooks/:provider; RateLimiter is optional (nil disables
// the middleware entirely, e.g. in tests that don't care about it).
// DynamicConfigStore/DynamicConfigCache/AdminAPIKey are Fase 4 additions
// backing the /admin/config surface — DynamicConfigStore nil disables that
// surface entirely (e.g. in tests that don't need it).
type Deps struct {
	Pool               *pgxpool.Pool
	QueueClient        *asynq.Client
	PaymentService     *payment.Service
	Providers          *provider.Registry
	Verifiers          map[string]auth.Verifier
	WebhookStore       *webhook.Store
	RateLimiter        *ratelimit.Limiter
	DynamicConfigStore *config.DynamicStore
	DynamicConfigCache *config.Cache
	AdminAPIKey        string
}

func NewApp(deps Deps) *fiber.App {
	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})

	RegisterHealthRoutes(app, deps)

	if deps.PaymentService != nil {
		paymentGroup := app.Group("/payments")
		if deps.RateLimiter != nil {
			paymentGroup.Use(ratelimit.Middleware(deps.RateLimiter, nil))
		}
		RegisterPaymentRoutes(paymentGroup, PaymentDeps{Service: deps.PaymentService})
		RegisterRefundRoutes(paymentGroup, PaymentDeps{Service: deps.PaymentService})
	}

	if deps.PaymentService != nil && deps.Providers != nil && deps.Verifiers != nil && deps.WebhookStore != nil {
		RegisterWebhookRoutes(app, WebhookDeps{
			Providers: deps.Providers,
			Verifiers: deps.Verifiers,
			Staging:   deps.WebhookStore,
			Service:   deps.PaymentService,
		})
	}

	if deps.DynamicConfigStore != nil {
		RegisterAdminRoutes(app, AdminDeps{
			Store:  deps.DynamicConfigStore,
			Cache:  deps.DynamicConfigCache,
			APIKey: deps.AdminAPIKey,
		})
	}

	return app
}

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
