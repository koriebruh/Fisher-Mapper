// Command server bootstraps config, a Postgres pool, a Redis-backed asynq
// client (used only for the /readyz health check -- see cmd/worker for the
// process that actually dispatches/consumes tasks), the PJP provider
// registry, the payment domain service, and a single fiber HTTP server
// exposing /healthz, /readyz, POST /payments, GET /payments/{id}, and (Fase
// 3) POST /webhooks/{provider} — wired as oklog/run actors with a signal
// handler so shutdown is deterministic. This process never calls a
// provider directly: create-payment only writes to Postgres (payment row +
// outbox row, one transaction).
//
// Startup order (per plan "Prinsip Arsitektur Dasar"):
//
//	bootstrap config -> observability (logger+tracer) -> connect Postgres ->
//	connect Redis -> register providers/domain services -> register transport
//	-> run actors
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"syscall"
	"time"

	"github.com/oklog/run"

	"Fisher-Mapper/internal/bootstrap"
	"Fisher-Mapper/internal/config"
	"Fisher-Mapper/internal/db"
	"Fisher-Mapper/internal/domain/payment"
	"Fisher-Mapper/internal/idempotency"
	"Fisher-Mapper/internal/lifecycle"
	"Fisher-Mapper/internal/queue"
	"Fisher-Mapper/internal/ratelimit"
	"Fisher-Mapper/internal/secrets/env"
	"Fisher-Mapper/internal/transport/rest"
	"Fisher-Mapper/internal/webhook"
)

const serviceName = "fisher-mapper"

// serverDynamicConfigRefreshInterval governs only THIS process's own Cache
// (used to serve /admin/config's GET and to best-effort-refresh right after
// a write) -- it has no bearing on how quickly cmd/worker's enforcement
// points observe a change; that is bounded by the worker's own Cache.Run
// interval, since they are different processes sharing only the DB.
const serverDynamicConfigRefreshInterval = 30 * time.Second

func main() {
	if err := run_(); err != nil {
		slog.Error("server exited with error", "error", err)
		os.Exit(1)
	}
}

func run_() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Bootstrap config: defaults -> config.toml -> env vars.
	cfg, err := config.Load(configPath())
	if err != nil {
		return fmt.Errorf("load bootstrap config: %w", err)
	}

	// 2. Observability: logger + tracer provider, registered explicitly
	// and in order before anything else connects or logs.
	obs, err := bootstrap.RegisterObservability(ctx, cfg, serviceName)
	if err != nil {
		return fmt.Errorf("register observability: %w", err)
	}
	logger := obs.Logger
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := obs.ShutdownTracer(shutdownCtx); err != nil {
			logger.Error("shutdown tracer provider", "error", err)
		}
	}()

	// 3. Connect Postgres.
	pool, err := db.NewPool(ctx, cfg.Postgres.DSN)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()
	logger.Info("connected to postgres")

	// 4. Connect Redis (asynq client). A failed connection here is not
	// fatal at process start per the plan ("gagal = tandai degraded,
	// lanjut") — /readyz will report not-ready until Redis is reachable.
	queueClient := queue.NewClient(cfg.Redis.Addr)
	defer func() {
		if err := queueClient.Close(); err != nil {
			logger.Error("close queue client", "error", err)
		}
	}()
	if err := queue.Ping(queueClient); err != nil {
		logger.Warn("redis not reachable at startup, continuing in degraded mode", "error", err)
	} else {
		logger.Info("connected to redis")
	}

	// 5. Domain wiring: providers (explicit registry, no blank import),
	// idempotency store, payment repository/service. This process is
	// HTTP-only (Fase 3 addendum): create-payment writes to Postgres only
	// (payment row + outbox row, one transaction) and never touches
	// Redis/the queue directly -- cmd/worker owns the outbox relay and the
	// actual provider calls.
	providers := bootstrap.RegisterProviders()
	idemStore := idempotency.NewPGStore(pool)
	paymentRepo := payment.NewPGRepository(pool)
	paymentService := payment.NewService(paymentRepo, idemStore, providers)
	verifiers := bootstrap.RegisterVerifiers(paymentRepo)
	webhookStore := webhook.NewStore(pool)
	limiter := ratelimit.New(20, 40) // stub-cheap default: 20 req/s, burst 40, per client/API key

	// Fase 4 dynamic config: this process only ever WRITES app_config (via
	// the admin endpoint below) and reads it back for that endpoint's GET
	// -- it never gates a provider call itself (create-payment never calls
	// a provider), so unlike cmd/worker it does not need to fail startup on
	// Load failing; a best-effort initial load is enough, and if it fails
	// the admin GET/PUT endpoints simply hit Postgres directly on demand
	// (DynamicStore.GetAll/SetWithAudit don't depend on the cache at all).
	dynamicStore := config.NewDynamicStore(pool)
	dynamicCache := config.NewCache(dynamicStore, serverDynamicConfigRefreshInterval)
	if err := dynamicCache.Load(ctx); err != nil {
		logger.Warn("initial dynamic config load failed, /admin/config still works (reads Postgres directly)", "error", err)
	}

	// Admin API key: same secrets.Secrets (env impl) pattern as the mock
	// webhook secret in bootstrap.RegisterVerifiers -- a documented,
	// local-dev-only fallback when unset, per the plan's "RBAC sederhana"
	// framing (one shared credential, not a full permission system).
	secretsStore := env.New("")
	adminAPIKey := secretsStore.GetSecret("admin_api_key")
	if adminAPIKey == "" {
		adminAPIKey = "dev-only-admin-key"
	}

	// 6. Transport: single fiber app with health + payment + webhook +
	// admin-config endpoints.
	app := rest.NewApp(rest.Deps{
		Pool:               pool,
		QueueClient:        queueClient,
		PaymentService:     paymentService,
		Providers:          providers,
		Verifiers:          verifiers,
		WebhookStore:       webhookStore,
		RateLimiter:        limiter,
		DynamicConfigStore: dynamicStore,
		DynamicConfigCache: dynamicCache,
		AdminAPIKey:        adminAPIKey,
	})

	addr := fmt.Sprintf(":%d", cfg.HTTP.Port)

	var g run.Group

	g.Add(run.SignalHandler(ctx, syscall.SIGINT, syscall.SIGTERM))

	fiberExecute, fiberInterrupt := lifecycle.FiberActor(app, addr, 5*time.Second)
	g.Add(fiberExecute, fiberInterrupt)

	// Fase 4 dynamic config watcher: without this actor, serverDynamicConfigRefreshInterval
	// drives no ticker at all -- this process's Cache would only ever update
	// via the admin PUT handler's best-effort post-write Refresh. The plan
	// lists "dynamic-config watcher" as an oklog/run actor unconditionally
	// (every long-running process that owns a Cache runs it), so this
	// process gets the same periodic background refresh cmd/worker does,
	// even though today nothing in cmd/server reads the cache on the hot
	// path (a future admin-surface read of the cache, or another
	// cache-backed feature added to this process, gets it for free).
	dynConfigExecute, dynConfigInterrupt := lifecycle.RunnerActor(dynamicCache.Run)
	g.Add(dynConfigExecute, dynConfigInterrupt)

	logger.Info("starting server", "addr", addr)
	if err := g.Run(); err != nil && !errors.Is(err, run.ErrSignal) {
		return err
	}
	logger.Info("shutdown complete")
	return nil
}

func configPath() string {
	if v := os.Getenv("APP_CONFIG_FILE"); v != "" {
		return v
	}
	return "configs/config.toml"
}
