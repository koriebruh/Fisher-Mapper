// Command server bootstraps config, a Postgres pool, a Redis-backed asynq
// client (used only for the /readyz health check -- see cmd/worker for the
// process that actually dispatches/consumes tasks), the PJP provider
// registry, the payment domain service, a fiber HTTP server exposing
// /healthz, /readyz, POST /payments, GET /payments/{id}, POST
// /webhooks/{provider} (Fase 3), and (Fase 6) a second gRPC listener
// exposing the same payment operations over internal/transport/grpc's
// PaymentServiceServer -- wired as oklog/run actors with a signal handler
// so shutdown is deterministic across both transports. This process never
// calls a provider directly: create-payment only writes to Postgres
// (payment row + outbox row, one transaction), regardless of which
// transport received the request.
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
	"net"
	"os"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/adaptor"
	"github.com/oklog/run"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"Fisher-Mapper/internal/domain/payment"
	"Fisher-Mapper/internal/messaging/idempotency"
	"Fisher-Mapper/internal/messaging/webhook"
	"Fisher-Mapper/internal/platform/bootstrap"
	"Fisher-Mapper/internal/platform/config"
	"Fisher-Mapper/internal/platform/db"
	"Fisher-Mapper/internal/platform/lifecycle"
	"Fisher-Mapper/internal/platform/observability"
	"Fisher-Mapper/internal/platform/queue"
	"Fisher-Mapper/internal/platform/secrets/env"
	"Fisher-Mapper/internal/resilience/ratelimit"
	grpctransport "Fisher-Mapper/internal/transport/grpc"
	paymentv1 "Fisher-Mapper/internal/transport/grpc/pb/payment/v1"
	"Fisher-Mapper/internal/transport/rest"
)

// grpcShutdownTimeout matches the fiber actor's own shutdown timeout below
// (lifecycle.FiberActor's 5*time.Second) -- both transports get the same
// grace period to finish in-flight requests on SIGINT/SIGTERM.
const grpcShutdownTimeout = 5 * time.Second

const serviceName = "fisher-mapper"

// serverDynamicConfigRefreshInterval governs only THIS process's own Cache
// (used to serve /admin/config's GET and to best-effort-refresh right after
// a write) -- it has no bearing on how quickly cmd/worker's enforcement
// points observe a change; that is bounded by the worker's own Cache.Run
// interval, since they are different processes sharing only the DB.
const serverDynamicConfigRefreshInterval = 30 * time.Second

// metricsPollInterval governs the Fase 5 DB-pool-stats poller actor (this
// process's own pgxpool.Pool -- it never writes terminal_failures, so unlike
// cmd/worker it has nothing to poll there).
const metricsPollInterval = 15 * time.Second

func main() {
	if err := run_(); err != nil {
		slog.Error("server exited with error", "error", err)
		os.Exit(1)
	}
}

func run_() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 0. Load .env (repo root), if present, BEFORE bootstrap config below --
	// its values must be in the process environment before config.Load's
	// env var overlay step (and LoadDynamicSeed) run.
	config.LoadDotEnv()

	// 1. Bootstrap config: defaults -> config.toml -> env vars.
	cfg, err := config.Load(configPath())
	if err != nil {
		return fmt.Errorf("load bootstrap config: %w", err)
	}

	// dynSeed carries the otel_enabled seed value only -- read directly from
	// config.toml because the tracer (step 2 below) is created before
	// Postgres connects, so there is no app_config row to read yet. See
	// config.DynamicSeed's doc for why this is a deliberate exception.
	dynSeed, err := config.LoadDynamicSeed(configPath())
	if err != nil {
		return fmt.Errorf("load dynamic config seed: %w", err)
	}

	// 2. Observability: logger + tracer provider, registered explicitly
	// and in order before anything else connects or logs.
	obs := bootstrap.RegisterObservability(ctx, cfg, serviceName, dynSeed.OtelEnabled)
	logger := obs.Logger
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := obs.Tracer.Shutdown(shutdownCtx); err != nil {
			logger.Error("shutdown tracer provider", "error", err)
		}
	}()

	// Fase 5 metrics: a Prometheus-backed MeterProvider, set globally BEFORE
	// rest.NewApp below constructs the otelfiber tracing middleware and this
	// process's own metrics middleware -- both resolve global providers at
	// construction time, so ordering here matters (see health.go's NewApp
	// doc). Never fails process startup: same "diagnostic, not load-bearing"
	// treatment as the tracer.
	meterProvider, promRegistry, err := observability.NewMeterProvider(ctx, serviceName)
	var metrics *observability.Metrics
	if err != nil {
		logger.Warn("observability: failed to build meter provider, metrics disabled", "error", err)
	} else {
		metrics, err = observability.NewMetrics(meterProvider.Meter(serviceName))
		if err != nil {
			logger.Warn("observability: failed to build metric instruments, metrics disabled", "error", err)
			metrics = nil
		}
	}
	if meterProvider != nil {
		defer func() {
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer shutdownCancel()
			if err := meterProvider.Shutdown(shutdownCtx); err != nil {
				logger.Error("shutdown meter provider", "error", err)
			}
		}()
	}

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
	} else {
		// Now that Postgres is reachable, the live app_config row (if any)
		// is the real source of truth for the OTel toggle -- reconcile the
		// tracer installed at step 2 against it. One-shot, not wired into
		// dynamicCache's periodic refresh (see TracerManager.Reconcile doc).
		obs.Tracer.Reconcile(ctx, dynamicCache.OtelEnabled(dynSeed.OtelEnabled))
	}

	// Admin API key: same secrets.Secrets (env impl) pattern as the mock
	// webhook secret in bootstrap.RegisterVerifiers. Deliberately NO
	// hardcoded fallback when unset -- adminAuth (rest/admin.go) already
	// rejects every request when its configured key is "", so leaving
	// adminAPIKey empty fails closed rather than falling open to a fixed,
	// source-visible literal (which would be a real auth bypass, not a
	// convenience default).
	secretsStore := env.New("")
	adminAPIKey := secretsStore.GetSecret("admin_api_key")
	if adminAPIKey == "" {
		logger.Warn("admin_api_key not configured; /admin/config will reject every request until it is set")
	}

	// Fase 5: GET /metrics, wrapped for fiber via middleware/adaptor so
	// rest.NewApp doesn't need its own prometheus/promhttp dependency.
	// promRegistry is nil if meter-provider construction failed above (fully
	// nil-safe: the handler and middleware are both simply omitted below).
	var metricsHandler fiber.Handler
	if promRegistry != nil {
		metricsHandler = adaptor.HTTPHandler(promhttp.HandlerFor(promRegistry, promhttp.HandlerOpts{}))
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
		Metrics:            metrics,
		MetricsHandler:     metricsHandler,
	})

	addr := fmt.Sprintf(":%d", cfg.HTTP.Port)

	// 7. gRPC transport (Fase 6): same paymentService instance as the fiber
	// app above -- "satu service layer, dua transport tipis". The listener
	// is bound here, before g.Run(), so a bad port fails startup loudly
	// instead of only surfacing once the run.Group actor's execute() runs.
	grpcAddr := fmt.Sprintf(":%d", cfg.GRPC.Port)
	grpcListener, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		return fmt.Errorf("listen grpc %s: %w", grpcAddr, err)
	}

	grpcServer := grpc.NewServer(
		// otelgrpc resolves the global TracerProvider (installed by
		// bootstrap.RegisterObservability above) at Handler-construction
		// time here, exactly like otelfiber.Middleware does for the fiber
		// app -- every RPC gets a span on the same trace pipeline REST uses.
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		// Same *ratelimit.Limiter instance the fiber payment group's
		// middleware uses (see rest.NewApp) -- gRPC is not a way around
		// REST's rate limiting.
		grpc.UnaryInterceptor(grpctransport.RateLimitInterceptor(limiter)),
	)
	paymentv1.RegisterPaymentServiceServer(grpcServer, grpctransport.NewServer(paymentService))
	// grpcurl (used for manual/validation testing) needs either the .proto
	// files or server reflection to resolve the service -- reflection is the
	// zero-config option for a template. In a real deployment this is worth
	// gating behind an internal-only listener/flag; that's a one-line change
	// (drop this call) left for the operator, not built here.
	reflection.Register(grpcServer)

	var g run.Group

	g.Add(run.SignalHandler(ctx, syscall.SIGINT, syscall.SIGTERM))

	fiberExecute, fiberInterrupt := lifecycle.FiberActor(app, addr, 5*time.Second)
	g.Add(fiberExecute, fiberInterrupt)

	grpcExecute, grpcInterrupt := lifecycle.GRPCServerActor(grpcServer, grpcListener, grpcShutdownTimeout)
	g.Add(grpcExecute, grpcInterrupt)

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

	// Fase 5 metrics poller: this process's own DB pool stats only (see
	// metricsPollInterval doc) -- no-op if metrics is nil (meter provider
	// failed to build above).
	if metrics != nil {
		poller := observability.NewPoller(pool, nil, metrics, metricsPollInterval)
		pollerExecute, pollerInterrupt := lifecycle.RunnerActor(poller.Run)
		g.Add(pollerExecute, pollerInterrupt)
	}

	logger.Info("starting server", "http_addr", addr, "grpc_addr", grpcAddr)
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
	return "config.toml"
}
