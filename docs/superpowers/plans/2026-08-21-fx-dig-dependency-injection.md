# fx/dig Dependency Injection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the manual, imperative wiring in `cmd/server/main.go`, `cmd/worker/main.go`, and `cmd/migrate/main.go` with a dependency-injection graph built on `go.uber.org/fx` (which is itself a lifecycle framework wrapped around `go.uber.org/dig` — there is no separate hand-rolled `dig.Container`), and replace `oklog/run.Group` with `fx.Lifecycle`.

**Architecture:** Every value `main()` currently builds imperatively (config, logger/tracer, Postgres pool, Redis/queue clients, domain services, transports) becomes an `fx.Provide` constructor. Every `oklog/run` actor (fiber, gRPC, dynamic-config watcher, metrics poller, outbox relay, asynq server, reconciliation job, worker metrics HTTP server) becomes an `fx.Invoke` function that registers an `fx.Lifecycle` hook. A single new helper, `fxbridge.Bridge`, adapts the existing `(execute func() error, interrupt func(error))` actor shape — already returned by every function in `internal/platform/lifecycle` — into an `fx.Hook`, so `internal/platform/lifecycle` itself does not change.

**Tech Stack:** Go 1.26, `go.uber.org/fx` (new dependency; pulls `go.uber.org/dig` transitively).

**Spec:** This plan has no separate spec document — requirements were established directly with the user: (1) full `fx.Lifecycle` replacing `oklog/run`, not just graph construction, and (2) convert all three binaries (`cmd/server`, `cmd/worker`, `cmd/migrate`), not just `cmd/server`.

## Global Constraints

- No behavior change. Startup order, shutdown order, degraded-mode fallbacks (Redis unreachable at boot, meter-provider construction failure), and every existing log line are preserved exactly. This is a structural refactor only.
- `fx.Lifecycle` `OnStop` hooks run in the reverse order their `OnStart`/hook was registered (LIFO) — this is what replaces every `defer` in the current `main()` functions. Register hooks in the same relative order the current code defers/adds actors, so shutdown order stays identical.
- dig only orders constructors by their declared parameters. Two places in the current code rely on an otel *global* being installed as a side effect of one step before another step's construction reads that global implicitly (`otel.SetMeterProvider` inside `observability.NewMeterProvider`, `otel.SetTracerProvider` inside `observability.NewTracerManager`, both consumed by `otelfiber`'s and `otelgrpc`'s middleware constructors). Any constructor with this kind of hidden dependency takes the upstream value as an explicit (possibly unused) parameter purely to force dig to build it first. Each occurrence is called out in the task below.
- `internal/platform/lifecycle`'s actor constructors (`FiberActor`, `GRPCServerActor`, `RunnerActor`, `AsynqServerActor`, `HTTPServerActor`) are not modified.
- `oklog/run` is removed from `cmd/server` and `cmd/worker` entirely (`fx.App.Run()` installs its own SIGINT/SIGTERM handler, replacing `run.SignalHandler`). `cmd/migrate` is a one-shot CLI, not a server — it uses `fx.Shutdowner` from inside an `fx.Lifecycle.OnStart` hook to end the process once the migration work is done, then `app.Run()` runs the queued `OnStop` hooks and exits. This is fx's documented pattern for short-lived applications.
- `fx.Lifecycle` and `fx.Shutdowner` are built into every `fx.App` automatically — never `fx.Provide`/`fx.Supply` them.
- `fx.NopLogger` is passed to every `fx.New(...)` call to suppress fx's own constructor/invoke event log (this project logs via `slog`; fx's default event logger would otherwise print unrelated startup noise to stdout).

## File Structure

- Create `internal/platform/fxbridge/bridge.go` — the one shared helper (`Bridge`) that adapts `(execute, interrupt)` into `fx.Hook`. Used by `cmd/server` and `cmd/worker`.
- Create `cmd/migrate/fx.go` — `fx.Provide`/`fx.Invoke` functions for the migrate CLI.
- Modify `cmd/migrate/main.go` — becomes a thin `fx.New(...).Run()` wrapper.
- Create `cmd/server/wire.go` — every `fx.Provide`/`fx.Invoke` function for the server binary.
- Modify `cmd/server/main.go` — becomes a thin `fx.New(...).Run()` wrapper.
- Create `cmd/worker/wire.go` — every `fx.Provide`/`fx.Invoke` function for the worker binary.
- Modify `cmd/worker/main.go` — becomes a thin `fx.New(...).Run()` wrapper.

`wire.go`/`fx.go` files stay in `package main` for each binary (not a shared `internal/` package): the provider functions are wiring details specific to one binary's dependency graph, matching how `main.go` already owned this logic.

---

### Task 1: `go.uber.org/fx` dependency + shared `fxbridge` helper

**Files:**
- Modify: `go.mod`, `go.sum`
- Create: `internal/platform/fxbridge/bridge.go`

**Interfaces:**
- Produces: `fxbridge.Bridge(lc fx.Lifecycle, sh fx.Shutdowner, execute func() error, interrupt func(error))` — used by every actor registration in Tasks 3 and 4.

- [ ] **Step 1: Add the dependency**

Run:
```bash
go get go.uber.org/fx
go mod tidy
```

- [ ] **Step 2: Create the bridge helper**

```go
// internal/platform/fxbridge/bridge.go

// Package fxbridge adapts internal/platform/lifecycle's (execute, interrupt)
// actor pairs -- the shape every actor in this repo already returns for
// oklog/run.Group.Add -- into fx.Hook, so cmd/server and cmd/worker can
// register the same actor constructors under fx.Lifecycle instead of
// run.Group, with no changes to internal/platform/lifecycle itself.
package fxbridge

import (
	"context"

	"go.uber.org/fx"
)

// Bridge appends a hook that starts execute in a goroutine on OnStart and
// calls interrupt on OnStop. OnStart must return immediately (fx blocks
// startup on it), so execute -- normally blocking, e.g. app.Listen -- cannot
// run inline. If execute returns a non-nil error (the actor crashed rather
// than shutting down cleanly), sh.Shutdown reports it to fx so the whole app
// exits with a non-zero code, matching oklog/run.Group's "any actor's
// execute() returning stops every actor" behavior.
func Bridge(lc fx.Lifecycle, sh fx.Shutdowner, execute func() error, interrupt func(error)) {
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go func() {
				if err := execute(); err != nil {
					_ = sh.Shutdown(fx.ExitCode(1))
				}
			}()
			return nil
		},
		OnStop: func(context.Context) error {
			interrupt(nil)
			return nil
		},
	})
}
```

- [ ] **Step 3: Verify it builds**

Run: `go build ./internal/platform/fxbridge/... && go vet ./internal/platform/fxbridge/...`
Expected: no output, exit 0.

- [ ] **Step 4: Commit**

```bash
git add go.mod go.sum internal/platform/fxbridge/bridge.go
git commit -m "build: add go.uber.org/fx, add fxbridge actor adapter"
```

---

### Task 2: Convert `cmd/migrate` (smallest binary — proves the one-shot fx pattern)

**Files:**
- Create: `cmd/migrate/fx.go`
- Modify: `cmd/migrate/main.go`

**Interfaces:**
- Consumes: `config.Load`, `config.LoadDotEnv`, `db.NewPool`, `db.NewMigrationHandle`, `db.RunMigrations`, `db.RollbackLastMigration`, `observability.NewLogger`, `tenantauth.NewStore(pool).CreateKey` — all unchanged, called from the same places, just moved into constructor functions.
- Produces: nothing consumed by other tasks (a fully separate binary).

- [ ] **Step 1: Write `cmd/migrate/fx.go`**

```go
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
```

- [ ] **Step 2: Rewrite `cmd/migrate/main.go`**

```go
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
```

- [ ] **Step 3: Build and vet**

Run: `go build ./cmd/migrate/... && go vet ./cmd/migrate/...`
Expected: no output, exit 0.

- [ ] **Step 4: Smoke test against a real Postgres**

Run (adjust DSN/env to match your local `.env`/`docker-compose.yml`):
```bash
go run ./cmd/migrate
go run ./cmd/migrate -down
go run ./cmd/migrate -create-tenant-key smoke-test-tenant
```
Expected: identical output/behavior to before this change — "migrations applied" / "last migration rolled back" / a printed API key — and exit code 0 in each case. Try an invalid DSN (e.g. `APP_CONFIG_FILE` pointing at a config with a bad `postgres.dsn`) and confirm the process still prints the connect error and exits 1.

- [ ] **Step 5: Commit**

```bash
git add cmd/migrate/fx.go cmd/migrate/main.go
git commit -m "refactor(migrate): wire cmd/migrate through fx/dig"
```

---

### Task 3: Convert `cmd/server`

**Files:**
- Create: `cmd/server/wire.go`
- Modify: `cmd/server/main.go`

**Interfaces:**
- Consumes: `fxbridge.Bridge` (Task 1); every existing constructor already used by the current `cmd/server/main.go` (`config.Load`, `bootstrap.RegisterObservability`, `bootstrap.RegisterProviders`, `bootstrap.RegisterVerifiers`, `db.NewPool`, `queue.NewClient`, `observability.NewMeterProvider`/`NewMetrics`, `idempotency.NewPGStore`, `payment.NewPGRepository`/`NewService`, `webhook.NewStore`, `ratelimit.New`, `tenantauth.NewStore`, `config.NewDynamicStore`/`NewCache`, `env.New`, `rest.NewApp`, `lifecycle.FiberActor`/`GRPCServerActor`/`RunnerActor`).
- Produces: nothing consumed by other tasks (a fully separate binary).

- [ ] **Step 1: Write `cmd/server/wire.go`**

```go
package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/adaptor"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"Fisher-Mapper/internal/domain/payment"
	"Fisher-Mapper/internal/messaging/idempotency"
	"Fisher-Mapper/internal/messaging/webhook"
	"Fisher-Mapper/internal/platform/bootstrap"
	"Fisher-Mapper/internal/platform/config"
	"Fisher-Mapper/internal/platform/db"
	"Fisher-Mapper/internal/platform/fxbridge"
	"Fisher-Mapper/internal/platform/lifecycle"
	"Fisher-Mapper/internal/platform/observability"
	"Fisher-Mapper/internal/platform/queue"
	"Fisher-Mapper/internal/platform/secrets/env"
	"Fisher-Mapper/internal/platform/tenantauth"
	"Fisher-Mapper/internal/provider"
	"Fisher-Mapper/internal/provider/auth"
	"Fisher-Mapper/internal/resilience/ratelimit"
	grpctransport "Fisher-Mapper/internal/transport/grpc"
	paymentv1 "Fisher-Mapper/internal/transport/grpc/pb/payment/v1"
	"Fisher-Mapper/internal/transport/rest"
)

// ServiceName/AdminAPIKey/RateLimitEnabledFunc wrap plain types so dig never
// confuses them with any other provided string/func() bool.
type ServiceName string
type AdminAPIKey string
type RateLimitEnabledFunc func() bool

func provideConfig() (config.Bootstrap, error) {
	config.LoadDotEnv()
	return config.Load(configPath())
}

func provideServiceName(cfg config.Bootstrap) ServiceName {
	return ServiceName(cfg.Service.Name)
}

func provideDynamicSeed() (config.DynamicSeed, error) {
	return config.LoadDynamicSeed(configPath())
}

func provideObservability(cfg config.Bootstrap, serviceName ServiceName, dynSeed config.DynamicSeed, lc fx.Lifecycle) bootstrap.Observability {
	obs := bootstrap.RegisterObservability(context.Background(), cfg, string(serviceName), dynSeed.OtelEnabled)
	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if err := obs.Tracer.Shutdown(shutdownCtx); err != nil {
				obs.Logger.Error("shutdown tracer provider", "error", err)
			}
			return nil
		},
	})
	return obs
}

// provideMeterProvider installs the global otel MeterProvider as a side
// effect (inside observability.NewMeterProvider). provideRestApp and
// provideGRPCServer below both take *sdkmetric.MeterProvider (or
// bootstrap.Observability) as an unused parameter purely to force dig to
// build the relevant constructor -- and install that global -- before
// otelfiber/otelgrpc's middleware resolves it.
func provideMeterProvider(serviceName ServiceName, obs bootstrap.Observability, lc fx.Lifecycle) (*sdkmetric.MeterProvider, *prometheus.Registry, *observability.Metrics) {
	meterProvider, promRegistry, err := observability.NewMeterProvider(context.Background(), string(serviceName))
	if err != nil {
		obs.Logger.Warn("observability: failed to build meter provider, metrics disabled", "error", err)
		return nil, nil, nil
	}

	metrics, err := observability.NewMetrics(meterProvider.Meter(string(serviceName)))
	if err != nil {
		obs.Logger.Warn("observability: failed to build metric instruments, metrics disabled", "error", err)
		metrics = nil
	}

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if err := meterProvider.Shutdown(shutdownCtx); err != nil {
				obs.Logger.Error("shutdown meter provider", "error", err)
			}
			return nil
		},
	})
	return meterProvider, promRegistry, metrics
}

func provideMetricsHandler(promRegistry *prometheus.Registry) fiber.Handler {
	if promRegistry == nil {
		return nil
	}
	return adaptor.HTTPHandler(promhttp.HandlerFor(promRegistry, promhttp.HandlerOpts{}))
}

func providePool(cfg config.Bootstrap, obs bootstrap.Observability, lc fx.Lifecycle) (*pgxpool.Pool, error) {
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
	obs.Logger.Info("connected to postgres")
	return pool, nil
}

func provideQueueClient(cfg config.Bootstrap, obs bootstrap.Observability, lc fx.Lifecycle) *asynq.Client {
	client := queue.NewClient(cfg.Redis.Addr)
	lc.Append(fx.Hook{
		OnStop: func(context.Context) error {
			if err := client.Close(); err != nil {
				obs.Logger.Error("close queue client", "error", err)
			}
			return nil
		},
	})
	if err := queue.Ping(client); err != nil {
		obs.Logger.Warn("redis not reachable at startup, continuing in degraded mode", "error", err)
	} else {
		obs.Logger.Info("connected to redis")
	}
	return client
}

func provideProviders() *provider.Registry {
	return bootstrap.RegisterProviders()
}

func provideIdempotencyStore(pool *pgxpool.Pool) *idempotency.PGStore {
	return idempotency.NewPGStore(pool)
}

func providePaymentRepo(pool *pgxpool.Pool) *payment.PGRepository {
	return payment.NewPGRepository(pool)
}

func providePaymentService(repo *payment.PGRepository, idemStore *idempotency.PGStore, providers *provider.Registry) *payment.Service {
	return payment.NewService(repo, idemStore, providers)
}

func provideVerifiers(paymentRepo *payment.PGRepository) map[string]auth.Verifier {
	return bootstrap.RegisterVerifiers(paymentRepo)
}

func provideWebhookStore(pool *pgxpool.Pool) *webhook.Store {
	return webhook.NewStore(pool)
}

func provideRateLimiter(cfg config.Bootstrap) *ratelimit.Limiter {
	return ratelimit.New(float64(cfg.Ratelimit.RatePerSecond), float64(cfg.Ratelimit.Burst))
}

func provideTenantAuthStore(pool *pgxpool.Pool) *tenantauth.Store {
	return tenantauth.NewStore(pool)
}

func provideDynamicStore(pool *pgxpool.Pool) *config.DynamicStore {
	return config.NewDynamicStore(pool)
}

// provideDynamicCache also performs the initial Load + tracer Reconcile
// side effect main() used to run inline right after constructing the cache
// -- both are one-shot actions tied to this value's construction, not
// ongoing behavior (the periodic refresh loop is a separate actor, see
// registerDynamicConfigActor).
func provideDynamicCache(cfg config.Bootstrap, dynSeed config.DynamicSeed, dynamicStore *config.DynamicStore, obs bootstrap.Observability) *config.Cache {
	dynamicCache := config.NewCache(dynamicStore, time.Duration(cfg.Server.DynamicConfigRefreshIntervalSeconds)*time.Second)
	ctx := context.Background()
	if err := dynamicCache.Load(ctx); err != nil {
		obs.Logger.Warn("initial dynamic config load failed, /admin/config still works (reads Postgres directly)", "error", err)
	} else {
		obs.Tracer.Reconcile(ctx, dynamicCache.OtelEnabled(dynSeed.OtelEnabled))
	}
	return dynamicCache
}

func provideRateLimitEnabledFunc(dynamicCache *config.Cache, dynSeed config.DynamicSeed) RateLimitEnabledFunc {
	return func() bool { return dynamicCache.RateLimitEnabled(dynSeed.RateLimitEnabled) }
}

func provideAdminAPIKey(obs bootstrap.Observability) AdminAPIKey {
	secretsStore := env.New("")
	adminAPIKey := secretsStore.GetSecret("admin_api_key")
	if adminAPIKey == "" {
		obs.Logger.Warn("admin_api_key not configured; /admin/config will reject every request until it is set")
	}
	return AdminAPIKey(adminAPIKey)
}

func provideCORSConfig(cfg config.Bootstrap) *cors.Config {
	if !cfg.CORS.Enabled {
		return nil
	}
	return &cors.Config{
		AllowOrigins:     cfg.CORS.AllowOrigins,
		AllowMethods:     cfg.CORS.AllowMethods,
		AllowHeaders:     cfg.CORS.AllowHeaders,
		ExposeHeaders:    cfg.CORS.ExposeHeaders,
		AllowCredentials: cfg.CORS.AllowCredentials,
		MaxAge:           cfg.CORS.MaxAgeSeconds,
	}
}

// provideRestApp's final parameter is unused -- see provideMeterProvider's
// doc for why it must still be declared here.
func provideRestApp(
	pool *pgxpool.Pool,
	queueClient *asynq.Client,
	paymentService *payment.Service,
	providers *provider.Registry,
	verifiers map[string]auth.Verifier,
	webhookStore *webhook.Store,
	limiter *ratelimit.Limiter,
	rateLimitEnabled RateLimitEnabledFunc,
	dynamicStore *config.DynamicStore,
	dynamicCache *config.Cache,
	adminAPIKey AdminAPIKey,
	tenantAuthStore *tenantauth.Store,
	metrics *observability.Metrics,
	metricsHandler fiber.Handler,
	corsConfig *cors.Config,
	_ *sdkmetric.MeterProvider,
) *fiber.App {
	return rest.NewApp(rest.Deps{
		Pool:               pool,
		QueueClient:        queueClient,
		PaymentService:     paymentService,
		Providers:          providers,
		Verifiers:          verifiers,
		WebhookStore:       webhookStore,
		RateLimiter:        limiter,
		RateLimitEnabled:   rateLimitEnabled,
		DynamicConfigStore: dynamicStore,
		DynamicConfigCache: dynamicCache,
		AdminAPIKey:        string(adminAPIKey),
		TenantAuthStore:    tenantAuthStore,
		Metrics:            metrics,
		MetricsHandler:     metricsHandler,
		CORS:               corsConfig,
	})
}

func provideGRPCListener(cfg config.Bootstrap) (net.Listener, error) {
	grpcAddr := fmt.Sprintf(":%d", cfg.GRPC.Port)
	return (&net.ListenConfig{}).Listen(context.Background(), "tcp", grpcAddr)
}

// provideGRPCServer's final parameter is unused -- see provideMeterProvider's
// doc; here it forces the global tracer provider (installed inside
// provideObservability) to exist before otelgrpc.NewServerHandler resolves
// it.
func provideGRPCServer(
	paymentService *payment.Service,
	limiter *ratelimit.Limiter,
	rateLimitEnabled RateLimitEnabledFunc,
	tenantAuthStore *tenantauth.Store,
	_ bootstrap.Observability,
) *grpc.Server {
	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(
			grpctransport.RateLimitInterceptor(limiter, rateLimitEnabled),
			grpctransport.TenantAuthInterceptor(tenantAuthStore),
		),
	)
	paymentv1.RegisterPaymentServiceServer(grpcServer, grpctransport.NewServer(paymentService))
	reflection.Register(grpcServer)
	return grpcServer
}

func registerFiberActor(lc fx.Lifecycle, sh fx.Shutdowner, app *fiber.App, cfg config.Bootstrap) {
	addr := fmt.Sprintf(":%d", cfg.HTTP.Port)
	shutdownTimeout := time.Duration(cfg.Server.ShutdownTimeoutSeconds) * time.Second
	execute, interrupt := lifecycle.FiberActor(app, addr, shutdownTimeout)
	fxbridge.Bridge(lc, sh, execute, interrupt)
}

func registerGRPCActor(lc fx.Lifecycle, sh fx.Shutdowner, grpcServer *grpc.Server, grpcListener net.Listener, cfg config.Bootstrap) {
	shutdownTimeout := time.Duration(cfg.Server.ShutdownTimeoutSeconds) * time.Second
	execute, interrupt := lifecycle.GRPCServerActor(grpcServer, grpcListener, shutdownTimeout)
	fxbridge.Bridge(lc, sh, execute, interrupt)
}

func registerDynamicConfigActor(lc fx.Lifecycle, sh fx.Shutdowner, dynamicCache *config.Cache) {
	execute, interrupt := lifecycle.RunnerActor(dynamicCache.Run)
	fxbridge.Bridge(lc, sh, execute, interrupt)
}

func registerMetricsPoller(lc fx.Lifecycle, sh fx.Shutdowner, pool *pgxpool.Pool, metrics *observability.Metrics, cfg config.Bootstrap) {
	if metrics == nil {
		return
	}
	poller := observability.NewPoller(pool, nil, metrics, time.Duration(cfg.Server.MetricsPollIntervalSeconds)*time.Second)
	execute, interrupt := lifecycle.RunnerActor(poller.Run)
	fxbridge.Bridge(lc, sh, execute, interrupt)
}

func logStartup(obs bootstrap.Observability, cfg config.Bootstrap) {
	addr := fmt.Sprintf(":%d", cfg.HTTP.Port)
	grpcAddr := fmt.Sprintf(":%d", cfg.GRPC.Port)
	obs.Logger.Info("starting server", "http_addr", addr, "grpc_addr", grpcAddr)
}

func configPath() string {
	if v := os.Getenv("APP_CONFIG_FILE"); v != "" {
		return v
	}
	return "config.toml"
}
```

- [ ] **Step 2: Rewrite `cmd/server/main.go`**

```go
// Command server bootstraps config, a Postgres pool, a Redis-backed asynq
// client (used only for the /readyz health check -- see cmd/worker for the
// process that actually dispatches/consumes tasks), the PJP provider
// registry, the payment domain service, a fiber HTTP server exposing
// /healthz, /readyz, POST /payments, GET /payments/{id}, POST
// /webhooks/{provider} (Fase 3), and (Fase 6) a second gRPC listener
// exposing the same payment operations over internal/transport/grpc's
// PaymentServiceServer.
//
// The dependency graph (config -> observability -> postgres/redis ->
// domain services -> transports) and every long-running actor (fiber,
// grpc, dynamic-config watcher, metrics poller) are wired through
// go.uber.org/fx -- see wire.go. fx.App.Run() installs its own
// SIGINT/SIGTERM handling and drives fx.Lifecycle OnStart/OnStop in place
// of the previous oklog/run.Group.
package main

import (
	"log/slog"
	"os"

	"go.uber.org/fx"
)

func main() {
	app := fx.New(
		fx.Provide(
			provideConfig,
			provideServiceName,
			provideDynamicSeed,
			provideObservability,
			provideMeterProvider,
			provideMetricsHandler,
			providePool,
			provideQueueClient,
			provideProviders,
			provideIdempotencyStore,
			providePaymentRepo,
			providePaymentService,
			provideVerifiers,
			provideWebhookStore,
			provideRateLimiter,
			provideTenantAuthStore,
			provideDynamicStore,
			provideDynamicCache,
			provideRateLimitEnabledFunc,
			provideAdminAPIKey,
			provideCORSConfig,
			provideRestApp,
			provideGRPCListener,
			provideGRPCServer,
		),
		fx.Invoke(
			logStartup,
			registerFiberActor,
			registerGRPCActor,
			registerDynamicConfigActor,
			registerMetricsPoller,
		),
		fx.NopLogger,
	)

	if err := app.Err(); err != nil {
		slog.Error("[server] main: dependency graph failed to build", "component", "server", "error", err)
		os.Exit(1)
	}
	app.Run()
}
```

- [ ] **Step 3: Build and vet**

Run: `go build ./cmd/server/... && go vet ./cmd/server/...`
Expected: no output, exit 0. Fix any dig "missing dependency"/"cycle detected" errors by re-checking each `fx.Provide` function's parameter list against the struct field it feeds.

- [ ] **Step 4: Run existing tests**

Run: `go test ./...`
Expected: PASS, same as before this change (no test exercises `main()` directly, but `internal/platform/config`, `internal/transport/rest`, etc. must be unaffected).

- [ ] **Step 5: Manual smoke test**

Run: `go run ./cmd/server` (against a local Postgres/Redis, e.g. `docker compose up -d postgres redis` first per `docker-compose.yml`).
Then, in another shell:
```bash
curl -i localhost:8080/healthz
curl -i localhost:8080/readyz
```
Expected: same responses as before this change. Ctrl-C the server and confirm it logs a clean shutdown (fiber + grpc both stop, "shutdown complete"-equivalent behavior) rather than hanging or panicking.

- [ ] **Step 6: Commit**

```bash
git add cmd/server/wire.go cmd/server/main.go
git commit -m "refactor(server): wire cmd/server through fx/dig, replace oklog/run with fx.Lifecycle"
```

---

### Task 4: Convert `cmd/worker`

**Files:**
- Create: `cmd/worker/wire.go`
- Modify: `cmd/worker/main.go`

**Interfaces:**
- Consumes: `fxbridge.Bridge` (Task 1); every existing constructor already used by the current `cmd/worker/main.go`, plus `circuitbreaker.NewRegistry`, `bulkhead.New`, `outbox.NewStore`/`NewRelay`, `queue.NewTerminalFailureRecorder`/`NewMemoryClient`/`NewAsynqClient`/`NewRedisHealthChecker`/`NewSwitchingClient`, `reconciliation.New`, `asynq.NewServeMux`/`NewServer`.
- Produces: nothing consumed by other tasks. `httpCallbackNotifier` and `callbackHTTPClient` (package-level vars, unrelated to the DI graph) are untouched.

- [ ] **Step 1: Write `cmd/worker/wire.go`**

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/fx"

	"Fisher-Mapper/internal/domain/payment"
	"Fisher-Mapper/internal/messaging/idempotency"
	"Fisher-Mapper/internal/messaging/outbox"
	"Fisher-Mapper/internal/messaging/reconciliation"
	"Fisher-Mapper/internal/messaging/webhook"
	"Fisher-Mapper/internal/platform/bootstrap"
	"Fisher-Mapper/internal/platform/config"
	"Fisher-Mapper/internal/platform/db"
	"Fisher-Mapper/internal/platform/fxbridge"
	"Fisher-Mapper/internal/platform/lifecycle"
	"Fisher-Mapper/internal/platform/observability"
	"Fisher-Mapper/internal/platform/queue"
	"Fisher-Mapper/internal/provider"
	"Fisher-Mapper/internal/resilience/bulkhead"
	"Fisher-Mapper/internal/resilience/circuitbreaker"
)

type ServiceName string
type QueueName string

// taskHandlers groups the three payload-unmarshal-then-dispatch closures so
// both provideMemoryClient and provideServeMux build their handler funcs
// from one place instead of duplicating the same three closures twice, the
// way the pre-fx main() did inline.
type taskHandlers struct {
	charge func(ctx context.Context, payload []byte) error
	refund func(ctx context.Context, payload []byte) error
	payout func(ctx context.Context, payload []byte) error
}

func provideConfig() (config.Bootstrap, error) {
	config.LoadDotEnv()
	return config.Load(configPath())
}

func provideServiceName(cfg config.Bootstrap) ServiceName {
	return ServiceName(cfg.Service.Name + "-worker")
}

func provideDynamicSeed() (config.DynamicSeed, error) {
	return config.LoadDynamicSeed(configPath())
}

func provideObservability(cfg config.Bootstrap, serviceName ServiceName, dynSeed config.DynamicSeed, lc fx.Lifecycle) bootstrap.Observability {
	obs := bootstrap.RegisterObservability(context.Background(), cfg, string(serviceName), dynSeed.OtelEnabled)
	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if err := obs.Tracer.Shutdown(shutdownCtx); err != nil {
				obs.Logger.Error("shutdown tracer provider", "error", err)
			}
			return nil
		},
	})
	return obs
}

// provideMeterProvider installs the global otel MeterProvider as a side
// effect. Nothing in cmd/worker needs to force-order against that global
// the way cmd/server's rest/grpc constructors do (this process has no
// otelfiber/otelgrpc middleware), so no consumer here takes an unused
// *sdkmetric.MeterProvider parameter -- it's still provided so
// provideMetricsServer/registerMetricsPoller can use promRegistry/metrics.
func provideMeterProvider(serviceName ServiceName, obs bootstrap.Observability, lc fx.Lifecycle) (*sdkmetric.MeterProvider, *prometheus.Registry, *observability.Metrics) {
	meterProvider, promRegistry, err := observability.NewMeterProvider(context.Background(), string(serviceName))
	if err != nil {
		obs.Logger.Warn("observability: failed to build meter provider, metrics disabled", "error", err)
		return nil, nil, nil
	}

	metrics, err := observability.NewMetrics(meterProvider.Meter(string(serviceName)))
	if err != nil {
		obs.Logger.Warn("observability: failed to build metric instruments, metrics disabled", "error", err)
		metrics = nil
	}

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if err := meterProvider.Shutdown(shutdownCtx); err != nil {
				obs.Logger.Error("shutdown meter provider", "error", err)
			}
			return nil
		},
	})
	return meterProvider, promRegistry, metrics
}

func providePool(cfg config.Bootstrap, obs bootstrap.Observability, lc fx.Lifecycle) (*pgxpool.Pool, error) {
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
	obs.Logger.Info("connected to postgres")
	return pool, nil
}

func provideDynamicStore(pool *pgxpool.Pool) *config.DynamicStore {
	return config.NewDynamicStore(pool)
}

// provideDynamicCache's initial Load is fatal on error (unlike cmd/server's
// soft-warn) -- there is no last-known-good snapshot at t=0, matching
// db.NewPool already hard-failing on its own Ping. See
// internal/platform/config.Cache.Run's doc for why every refresh AFTER this
// one falls back to the last-known-good snapshot instead.
func provideDynamicCache(cfg config.Bootstrap, dynSeed config.DynamicSeed, dynamicStore *config.DynamicStore, obs bootstrap.Observability) (*config.Cache, error) {
	dynamicCache := config.NewCache(dynamicStore, time.Duration(cfg.Worker.DynamicConfigRefreshIntervalSeconds)*time.Second)
	ctx := context.Background()
	if err := dynamicCache.Load(ctx); err != nil {
		return nil, fmt.Errorf("load dynamic config: %w", err)
	}
	obs.Logger.Info("loaded dynamic config")
	obs.Tracer.Reconcile(ctx, dynamicCache.OtelEnabled(dynSeed.OtelEnabled))
	return dynamicCache, nil
}

// provideQueueName reads the queue name once, exactly like the pre-fx
// main() did -- see outbox.Relay/asynq.Server's doc on why a live getter
// would let producer and consumer disagree on the queue mid-process.
func provideQueueName(dynamicCache *config.Cache, dynSeed config.DynamicSeed, obs bootstrap.Observability) QueueName {
	queueName := dynamicCache.QueueName(dynSeed.QueueDefaultName)
	obs.Logger.Info("queue name resolved", "queue", queueName)
	return QueueName(queueName)
}

func provideProviders() *provider.Registry {
	return bootstrap.RegisterProviders()
}

func provideIdempotencyStore(pool *pgxpool.Pool) *idempotency.PGStore {
	return idempotency.NewPGStore(pool)
}

func providePaymentRepo(pool *pgxpool.Pool) *payment.PGRepository {
	return payment.NewPGRepository(pool)
}

func provideWebhookStore(pool *pgxpool.Pool) *webhook.Store {
	return webhook.NewStore(pool)
}

func provideBreakers(cfg config.Bootstrap) *circuitbreaker.Registry {
	return circuitbreaker.NewRegistry(cfg.Worker.BreakerFailureThreshold, time.Duration(cfg.Worker.BreakerCooldownSeconds)*time.Second)
}

func provideBulkheadLimiter(cfg config.Bootstrap) *bulkhead.Limiter {
	return bulkhead.New(cfg.Worker.BulkheadCapacityPerProvider)
}

func providePaymentService(
	repo *payment.PGRepository,
	idemStore *idempotency.PGStore,
	providers *provider.Registry,
	webhookStore *webhook.Store,
	breakers *circuitbreaker.Registry,
	bulkheadLimiter *bulkhead.Limiter,
	dynamicCache *config.Cache,
	dynSeed config.DynamicSeed,
	metrics *observability.Metrics,
) *payment.Service {
	svc := payment.NewService(repo, idemStore, providers).
		WithWebhookStaging(webhookStore).
		WithCircuitBreakers(breakers).
		WithBulkhead(bulkheadLimiter).
		WithProviderEnabledCheck(dynamicCache.ProviderEnabled).
		WithCircuitBreakerEnabledCheck(func() bool { return dynamicCache.CircuitBreakerEnabled(dynSeed.CircuitBreakerEnabled) }).
		WithCallbackNotifier(httpCallbackNotifier)
	if metrics != nil {
		svc = svc.WithReconciliationMismatchHook(func(ctx context.Context) {
			metrics.ReconciliationMismatch.Add(ctx, 1)
		})
	}
	return svc
}

func provideTaskHandlers(paymentService *payment.Service) taskHandlers {
	return taskHandlers{
		charge: func(ctx context.Context, payload []byte) error {
			var in payment.ChargeTaskInput
			if err := json.Unmarshal(payload, &in); err != nil {
				return fmt.Errorf("worker: unmarshal charge task payload: %w", err)
			}
			return paymentService.ProcessCharge(ctx, in)
		},
		refund: func(ctx context.Context, payload []byte) error {
			var in payment.RefundTaskInput
			if err := json.Unmarshal(payload, &in); err != nil {
				return fmt.Errorf("worker: unmarshal refund task payload: %w", err)
			}
			return paymentService.ProcessRefund(ctx, in)
		},
		payout: func(ctx context.Context, payload []byte) error {
			var in payment.PayoutTaskInput
			if err := json.Unmarshal(payload, &in); err != nil {
				return fmt.Errorf("worker: unmarshal payout task payload: %w", err)
			}
			return paymentService.ProcessPayout(ctx, in)
		},
	}
}

func provideTerminalFailures(pool *pgxpool.Pool) *queue.TerminalFailureRecorder {
	return queue.NewTerminalFailureRecorder(pool)
}

func provideMemoryClient(handlers taskHandlers, terminalFailures *queue.TerminalFailureRecorder) *queue.MemoryClient {
	memoryClient := queue.NewMemoryClient(terminalFailures.MemoryErrorRecorder())
	memoryClient.RegisterHandler(queue.TaskTypeCharge, func(ctx context.Context, _ string, payload []byte) error {
		return handlers.charge(ctx, payload)
	})
	memoryClient.RegisterHandler(queue.TaskTypeRefund, func(ctx context.Context, _ string, payload []byte) error {
		return handlers.refund(ctx, payload)
	})
	memoryClient.RegisterHandler(queue.TaskTypePayout, func(ctx context.Context, _ string, payload []byte) error {
		return handlers.payout(ctx, payload)
	})
	return memoryClient
}

func provideSwitchingClient(cfg config.Bootstrap, memoryClient *queue.MemoryClient, obs bootstrap.Observability, lc fx.Lifecycle) *queue.SwitchingClient {
	asynqClient := queue.NewAsynqClient(cfg.Redis.Addr)
	redisHealth := queue.NewRedisHealthChecker(cfg.Redis.Addr, time.Duration(cfg.Worker.RedisHealthIntervalSeconds)*time.Second)
	switchingClient := queue.NewSwitchingClient(asynqClient, memoryClient, redisHealth)
	lc.Append(fx.Hook{
		OnStop: func(context.Context) error {
			if err := switchingClient.Close(); err != nil {
				obs.Logger.Error("close queue client", "error", err)
			}
			return nil
		},
	})
	return switchingClient
}

func provideOutboxStore(pool *pgxpool.Pool) *outbox.Store {
	return outbox.NewStore(pool)
}

func provideRelay(cfg config.Bootstrap, outboxStore *outbox.Store, switchingClient *queue.SwitchingClient, dynamicCache *config.Cache, queueName QueueName, metrics *observability.Metrics) *outbox.Relay {
	relay := outbox.NewRelay(outboxStore, switchingClient,
		time.Duration(cfg.Worker.RelayBaseIntervalSeconds)*time.Second,
		time.Duration(cfg.Worker.RelayMaxIntervalSeconds)*time.Second,
		cfg.Worker.RelayBatchSize).
		WithProviderEnabledCheck(dynamicCache.ProviderEnabled).
		WithQueueName(func() string { return string(queueName) })
	if metrics != nil {
		relay = relay.WithDispatchLagRecorder(func(ctx context.Context, taskType string, lag time.Duration) {
			metrics.OutboxDispatchLag.Record(ctx, lag.Seconds(), metric.WithAttributes(attribute.String("task_type", taskType)))
		})
	}
	return relay
}

func provideServeMux(handlers taskHandlers) *asynq.ServeMux {
	mux := asynq.NewServeMux()
	mux.HandleFunc(queue.TaskTypeCharge, func(ctx context.Context, task *asynq.Task) error {
		return handlers.charge(ctx, task.Payload())
	})
	mux.HandleFunc(queue.TaskTypeRefund, func(ctx context.Context, task *asynq.Task) error {
		return handlers.refund(ctx, task.Payload())
	})
	mux.HandleFunc(queue.TaskTypePayout, func(ctx context.Context, task *asynq.Task) error {
		return handlers.payout(ctx, task.Payload())
	})
	return mux
}

func provideReconciler(cfg config.Bootstrap, paymentService *payment.Service, dynamicCache *config.Cache, dynSeed config.DynamicSeed) *reconciliation.Job {
	return reconciliation.New(paymentService,
		time.Duration(cfg.Worker.ReconciliationPollIntervalSeconds)*time.Second,
		time.Duration(cfg.Worker.ReconciliationStuckThresholdSeconds)*time.Second).
		WithEnabledCheck(func() bool { return dynamicCache.ReconciliationEnabled(dynSeed.ReconciliationEnabled) })
}

func provideAsynqServer(cfg config.Bootstrap, queueName QueueName, terminalFailures *queue.TerminalFailureRecorder) *asynq.Server {
	return asynq.NewServer(
		asynq.RedisClientOpt{Addr: cfg.Redis.Addr},
		asynq.Config{
			Concurrency:  cfg.Worker.AsynqConcurrency,
			Queues:       map[string]int{string(queueName): 1},
			ErrorHandler: terminalFailures.AsynqErrorHandler(),
		},
	)
}

// provideMetricsServer returns nil when promRegistry is nil (meter provider
// failed to build) -- every consumer of *http.Server below is nil-safe.
func provideMetricsServer(cfg config.Bootstrap, promRegistry *prometheus.Registry) *http.Server {
	if promRegistry == nil {
		return nil
	}
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.HandlerFor(promRegistry, promhttp.HandlerOpts{}))
	return &http.Server{
		Addr:              ":" + cfg.Worker.MetricsPort,
		Handler:           metricsMux,
		ReadHeaderTimeout: 5 * time.Second,
	}
}

func registerRelayActor(lc fx.Lifecycle, sh fx.Shutdowner, relay *outbox.Relay) {
	execute, interrupt := lifecycle.RunnerActor(relay.Run)
	fxbridge.Bridge(lc, sh, execute, interrupt)
}

func registerAsynqActor(lc fx.Lifecycle, sh fx.Shutdowner, asynqServer *asynq.Server, mux *asynq.ServeMux) {
	execute, interrupt := lifecycle.AsynqServerActor(asynqServer, mux)
	fxbridge.Bridge(lc, sh, execute, interrupt)
}

func registerDynamicConfigActor(lc fx.Lifecycle, sh fx.Shutdowner, dynamicCache *config.Cache) {
	execute, interrupt := lifecycle.RunnerActor(dynamicCache.Run)
	fxbridge.Bridge(lc, sh, execute, interrupt)
}

func registerReconcileActor(lc fx.Lifecycle, sh fx.Shutdowner, reconciler *reconciliation.Job) {
	execute, interrupt := lifecycle.RunnerActor(reconciler.Run)
	fxbridge.Bridge(lc, sh, execute, interrupt)
}

func registerMetricsPoller(lc fx.Lifecycle, sh fx.Shutdowner, pool *pgxpool.Pool, terminalFailures *queue.TerminalFailureRecorder, metrics *observability.Metrics, cfg config.Bootstrap) {
	if metrics == nil {
		return
	}
	poller := observability.NewPoller(pool, terminalFailures.Count, metrics, time.Duration(cfg.Worker.MetricsPollIntervalSeconds)*time.Second)
	execute, interrupt := lifecycle.RunnerActor(poller.Run)
	fxbridge.Bridge(lc, sh, execute, interrupt)
}

func registerMetricsServerActor(lc fx.Lifecycle, sh fx.Shutdowner, metricsSrv *http.Server, obs bootstrap.Observability) {
	if metricsSrv == nil {
		return
	}
	execute, interrupt := lifecycle.HTTPServerActor(metricsSrv, 5*time.Second)
	fxbridge.Bridge(lc, sh, execute, interrupt)
	obs.Logger.Info("worker metrics endpoint listening", "addr", metricsSrv.Addr)
}

func logStartup(obs bootstrap.Observability, cfg config.Bootstrap) {
	obs.Logger.Info("starting worker", "redis_addr", cfg.Redis.Addr)
}

func configPath() string {
	if v := os.Getenv("APP_CONFIG_FILE"); v != "" {
		return v
	}
	return "config.toml"
}
```

- [ ] **Step 2: Rewrite `cmd/worker/main.go`**

Keep `callbackHTTPClient` and `httpCallbackNotifier` exactly as they are today (package-level, unrelated to the DI graph) — only the `main`/`runWorker` function is replaced:

```go
// Command worker is the Fase 3 asynq worker process: it connects Postgres
// and Redis, wires the same provider registry + payment domain service as
// cmd/server (via the same bootstrap.* helpers, so there is exactly one
// source of truth for that wiring), and runs the outbox relay (Postgres ->
// queue), the task server that calls into payment.Service.ProcessCharge
// (queue -> provider), and the reconciliation job.
//
// Deliberately a separate binary from cmd/server (per the plan's directory
// structure): cmd/server is HTTP-only and never touches Redis on the
// request path; this process is the only thing that ever calls a
// provider's Charge/Authorize method.
//
// The dependency graph and every long-running actor (relay, asynq server,
// dynamic-config watcher, reconciliation job, metrics poller, metrics HTTP
// server) are wired through go.uber.org/fx -- see wire.go.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"time"

	"go.uber.org/fx"

	"Fisher-Mapper/internal/domain/payment"
	"Fisher-Mapper/internal/resilience/ssrf"
)

func main() {
	app := fx.New(
		fx.Provide(
			provideConfig,
			provideServiceName,
			provideDynamicSeed,
			provideObservability,
			provideMeterProvider,
			providePool,
			provideDynamicStore,
			provideDynamicCache,
			provideQueueName,
			provideProviders,
			provideIdempotencyStore,
			providePaymentRepo,
			provideWebhookStore,
			provideBreakers,
			provideBulkheadLimiter,
			providePaymentService,
			provideTaskHandlers,
			provideTerminalFailures,
			provideMemoryClient,
			provideSwitchingClient,
			provideOutboxStore,
			provideRelay,
			provideServeMux,
			provideReconciler,
			provideAsynqServer,
			provideMetricsServer,
		),
		fx.Invoke(
			logStartup,
			registerRelayActor,
			registerAsynqActor,
			registerDynamicConfigActor,
			registerReconcileActor,
			registerMetricsPoller,
			registerMetricsServerActor,
		),
		fx.NopLogger,
	)

	if err := app.Err(); err != nil {
		slog.Error("[worker] main: dependency graph failed to build", "component", "worker", "error", err)
		os.Exit(1)
	}
	app.Run()
}

// callbackHTTPClient is shared across every delivery attempt. Its Timeout
// IS the 5s bound the task doc requires, not something the caller context
// needs to enforce (see payment.CallbackNotifier's doc on why ProcessCharge/
// ProcessPayout pass a detached context.Background() here).
//
// SSRF: callback_url is entirely caller-supplied, so this client's
// Transport dials through ssrf.SafeDialer -- its Control hook rejects the
// connection at the point Go is about to connect(2) to the ACTUAL resolved
// address, which is what makes it safe against DNS rebinding (a pre-flight
// lookup, checked once and then discarded, would not be). CheckRedirect
// refuses every redirect for the identical reason: a validated-safe public
// URL could otherwise 302 to an internal address and the redirect would be
// followed with no re-check.
var callbackHTTPClient = &http.Client{
	Timeout: 5 * time.Second,
	Transport: &http.Transport{
		DialContext: ssrf.SafeDialer(5 * time.Second).DialContext,
	},
	CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

// httpCallbackNotifier is the concrete payment.CallbackNotifier this
// process wires in: a single best-effort POST, no retry/backoff/signing
// (explicitly deferred -- Name-and-Skip tier, see the task's step 6 scope).
// A delivery failure (network error, non-2xx) is logged and otherwise
// swallowed -- it must never fail the ProcessCharge/ProcessPayout task that
// triggered it or cause a queue retry.
func httpCallbackNotifier(ctx context.Context, url string, payload payment.CallbackPayload) {
	body, err := json.Marshal(payload)
	if err != nil {
		slog.Error("[worker] httpCallbackNotifier: marshal payload", "component", "worker", "url", url, "error", err)
		return
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		slog.Error("[worker] httpCallbackNotifier: build request", "component", "worker", "url", url, "error", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := callbackHTTPClient.Do(req)
	if err != nil {
		slog.Warn("[worker] httpCallbackNotifier: delivery failed", "component", "worker", "url", url, "error", err)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		slog.Warn("[worker] httpCallbackNotifier: non-2xx response", "component", "worker", "url", url, "status", resp.StatusCode)
	}
}
```

- [ ] **Step 3: Build and vet**

Run: `go build ./cmd/worker/... && go vet ./cmd/worker/...`
Expected: no output, exit 0.

- [ ] **Step 4: Run existing tests**

Run: `go test ./...`
Expected: PASS.

- [ ] **Step 5: Manual smoke test**

Run: `go run ./cmd/worker` (against the same local Postgres/Redis as Task 3).
Expected: logs "connected to postgres", "loaded dynamic config", "queue name resolved", "starting worker", and (if `cfg.Worker.MetricsPort` is set) "worker metrics endpoint listening". Ctrl-C and confirm a clean shutdown log, matching the pre-fx behavior.

Enqueue a real task end-to-end if a full local stack is available (`cmd/server` + `cmd/worker` + Postgres + Redis via `docker-compose.yml`): POST a payment through `cmd/server`, confirm `cmd/worker` picks it up off the outbox/queue and processes it exactly as before.

- [ ] **Step 6: Commit**

```bash
git add cmd/worker/wire.go cmd/worker/main.go
git commit -m "refactor(worker): wire cmd/worker through fx/dig, replace oklog/run with fx.Lifecycle"
```

---

### Task 5: Repo-wide verification and cleanup

**Files:** none (verification only)

- [ ] **Step 1: Full build/vet/test**

Run: `go build ./... && go vet ./... && go test ./...`
Expected: all green.

- [ ] **Step 2: Confirm `oklog/run` is gone from the three binaries but still declared correctly in go.mod**

Run: `grep -rn "oklog/run" cmd/`
Expected: no matches. (If `internal/platform/lifecycle`'s doc comment still references `oklog/run.Group` conceptually, that's fine — the actor *shape* it documents is unchanged, only the caller changed; leave that package's doc comment as-is unless it makes an explicitly false claim after this refactor, in which case update it in this task.)

Run: `grep -n "oklog/run" go.mod`
Decide: if nothing outside `cmd/` still imports `oklog/run`, run `go mod tidy` to drop it from `go.mod`/`go.sum`.

- [ ] **Step 3: golangci-lint (if configured in CI)**

Run: `golangci-lint run ./...` (matches `.golangci.yml` at the repo root, per CI).
Expected: no new findings introduced by this refactor.

- [ ] **Step 4: Update `internal/platform/lifecycle`'s package doc if it names `oklog/run` as the only caller**

Read `internal/platform/lifecycle/lifecycle.go`'s package doc (`// Package lifecycle wires long-running processes as oklog/run actors...`). If, after this refactor, no code path uses `oklog/run` anymore, reword it to describe the actor shape generically (used by both `oklog/run.Group.Add` in the past and now `fxbridge.Bridge`) rather than naming a framework that's no longer in the call graph. Keep this a doc-only change — the actual `(execute, interrupt)` function signatures do not change.

- [ ] **Step 5: Final commit**

```bash
git add -A
git commit -m "chore: drop oklog/run dependency, tidy go.mod after fx/dig migration"
```
