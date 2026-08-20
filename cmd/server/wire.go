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
