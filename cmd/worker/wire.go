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

// provideDynamicSeed takes config.Bootstrap as an unused parameter to force dig
// to resolve provideConfig first. provideConfig has a side effect (LoadDotEnv)
// that populates environment variables that configPath() reads (specifically APP_CONFIG_FILE).
// Without this dependency, dig might resolve provideDynamicSeed before provideConfig,
// causing configPath() to silently fall back to "config.toml" and LoadDynamicSeed
// to return defaults, reverting several feature-flag defaults with no error.
func provideDynamicSeed(_ config.Bootstrap) (config.DynamicSeed, error) {
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
