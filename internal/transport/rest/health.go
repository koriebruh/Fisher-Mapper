// Package rest holds the REST (fiber) transport. Phase 1 only wires the
// health-check surface; payment endpoints arrive in later phases.
package rest

import (
	"context"
	"log/slog"

	"github.com/gofiber/contrib/otelfiber/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgxpool"

	"Fisher-Mapper/internal/domain/payment"
	"Fisher-Mapper/internal/messaging/webhook"
	"Fisher-Mapper/internal/platform/config"
	"Fisher-Mapper/internal/platform/observability"
	"Fisher-Mapper/internal/platform/queue"
	"Fisher-Mapper/internal/platform/tenantauth"
	"Fisher-Mapper/internal/provider"
	"Fisher-Mapper/internal/provider/auth"
	"Fisher-Mapper/internal/resilience/ratelimit"
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
	Pool           *pgxpool.Pool
	QueueClient    *asynq.Client
	PaymentService *payment.Service
	Providers      *provider.Registry
	Verifiers      map[string]auth.Verifier
	WebhookStore   *webhook.Store
	RateLimiter    *ratelimit.Limiter
	// RateLimitEnabled is the dynamic-config ratelimit.enabled toggle,
	// checked live on every request -- nil means "always enabled" (same
	// default as RateLimiter being nil disabling the middleware entirely).
	RateLimitEnabled   func() bool
	DynamicConfigStore *config.DynamicStore
	DynamicConfigCache *config.Cache
	AdminAPIKey        string
	// TenantAuthStore backs tenantAuth, the CRITICAL-fix caller-authentication
	// middleware applied to the payment/refund/payout route groups below.
	// Unlike RateLimiter, this is NOT optional in practice once PaymentService
	// is set: tenantAuth fails closed (401) when Store is nil, same stance as
	// adminAuth's empty-APIKey handling -- there is no "auth disabled" mode
	// for a money-moving endpoint.
	TenantAuthStore *tenantauth.Store
	// Metrics and MetricsHandler are Fase 5 additions (either nil disables
	// the corresponding piece -- e.g. in tests that don't care about
	// metrics). Metrics backs the per-request latency middleware;
	// MetricsHandler is the actual GET /metrics route handler (a
	// promhttp.Handler wrapped for fiber via middleware/adaptor, built by
	// main() -- kept out of this package so it doesn't need its own
	// dependency on prometheus/promhttp or the exporter's registry). See
	// internal/platform/observability/metrics.go.
	Metrics        *observability.Metrics
	MetricsHandler fiber.Handler
	// CORS is nil-disables (matches every other optional Deps field's
	// convention): cmd/server builds it from Bootstrap.CORS, passing
	// nil when CORS.Enabled is false rather than a zero-value Config
	// (fiber's cors.New(cors.Config{}) still installs an origin-matching
	// middleware -- "disabled" has to mean "not registered at all").
	CORS *cors.Config
}

func NewApp(deps Deps) *fiber.App {
	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})

	// CORS must run before every other middleware: a browser's preflight
	// OPTIONS request carries no auth/idempotency headers of its own, so it
	// has to be answered (or rejected) before otelfiber/rate-limit/tenantAuth
	// ever see it.
	if deps.CORS != nil {
		app.Use(cors.New(*deps.CORS))
	}

	// otelfiber gives every request a span (Fase 5 item 2), using whatever
	// TracerProvider/Propagator are globally installed at THIS call time --
	// callers (cmd/server) must call bootstrap.RegisterObservability (which
	// sets both) before NewApp, or requests trace under a no-op provider
	// with no error anywhere. Metrics are handled by our own middleware
	// below (RegisterMetricsMiddleware), not otelfiber's built-in module --
	// WithoutMetrics avoids shipping two competing HTTP-latency histograms.
	app.Use(otelfiber.Middleware(otelfiber.WithoutMetrics(true)))

	if deps.Metrics != nil {
		RegisterMetricsMiddleware(app, deps.Metrics)
	}
	if deps.MetricsHandler != nil {
		app.Get("/metrics", deps.MetricsHandler)
	}

	RegisterHealthRoutes(app, deps)

	if deps.PaymentService != nil {
		paymentGroup := app.Group("/payments")
		if deps.RateLimiter != nil {
			// Rate-limit before auth: tenantAuth does a Postgres lookup per
			// request, so an unauthenticated flood should be throttled
			// before it ever reaches that query, not after.
			paymentGroup.Use(ratelimit.Middleware(deps.RateLimiter, nil, deps.RateLimitEnabled))
		}
		paymentGroup.Use(tenantAuth(deps.TenantAuthStore))
		RegisterPaymentRoutes(paymentGroup, PaymentDeps{Service: deps.PaymentService})
		RegisterRefundRoutes(paymentGroup, PaymentDeps{Service: deps.PaymentService})

		// payouts is its own top-level group (not a /payments sub-route):
		// unlike a refund, a payout is standalone -- it has no parent payment
		// id to hang a URL off of.
		payoutGroup := app.Group("/payouts")
		if deps.RateLimiter != nil {
			payoutGroup.Use(ratelimit.Middleware(deps.RateLimiter, nil, deps.RateLimitEnabled))
		}
		payoutGroup.Use(tenantAuth(deps.TenantAuthStore))
		RegisterPayoutRoutes(payoutGroup, PaymentDeps{Service: deps.PaymentService})
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
// keyStatus is the JSON key every /healthz and /readyz response reports its
// outcome under.
const keyStatus = "status"

func RegisterHealthRoutes(app *fiber.App, deps Deps) {
	app.Get("/healthz", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{keyStatus: "ok"})
	})

	app.Get("/readyz", func(c *fiber.Ctx) error {
		ctx := c.Context()

		if err := checkPostgres(ctx, deps.Pool); err != nil {
			slog.Warn("[rest] RegisterHealthRoutes: readyz postgres not ready", "component", "rest", "error", err)
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				keyStatus: "not ready",
				"reason":  "postgres",
			})
		}

		if err := checkRedis(deps.QueueClient); err != nil {
			slog.Warn("[rest] RegisterHealthRoutes: readyz redis not ready", "component", "rest", "error", err)
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				keyStatus: "not ready",
				"reason":  "redis",
			})
		}

		return c.JSON(fiber.Map{keyStatus: "ok"})
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
