// Command worker is the Fase 3 asynq worker process: it connects Postgres
// and Redis, wires the same provider registry + payment domain service as
// cmd/server (via the same bootstrap.* helpers, so there is exactly one
// source of truth for that wiring), and runs two long-running actors under
// oklog/run.Group: the outbox relay (Postgres -> queue) and the task
// server that actually calls into payment.Service.ProcessCharge (queue ->
// provider).
//
// Deliberately a separate binary from cmd/server (per the plan's directory
// structure): cmd/server is HTTP-only and never touches Redis on the
// request path; this process is the only thing that ever calls a
// provider's Charge/Authorize method.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"syscall"
	"time"

	"github.com/hibiken/asynq"
	"github.com/oklog/run"

	"Fisher-Mapper/internal/bootstrap"
	"Fisher-Mapper/internal/bulkhead"
	"Fisher-Mapper/internal/circuitbreaker"
	"Fisher-Mapper/internal/config"
	"Fisher-Mapper/internal/db"
	"Fisher-Mapper/internal/domain/payment"
	"Fisher-Mapper/internal/idempotency"
	"Fisher-Mapper/internal/lifecycle"
	"Fisher-Mapper/internal/outbox"
	"Fisher-Mapper/internal/queue"
	"Fisher-Mapper/internal/reconciliation"
	"Fisher-Mapper/internal/webhook"
)

const serviceName = "fisher-mapper-worker"

// Defaults for the resilience stubs. These are exactly the kind of values
// the full plan puts in Fase 4's dynamic config (app_config) -- Fase 3
// hardcodes them here deliberately, since dynamic config does not exist
// yet and building it now would be scope creep for this phase.
const (
	breakerFailureThreshold = 5
	breakerCooldown         = 30 * time.Second
	bulkheadCapacityPerProv = 4
	asynqConcurrency        = 10
	relayBaseInterval       = 2 * time.Second
	relayMaxInterval        = 30 * time.Second
	relayBatchSize          = 50
	redisHealthInterval     = 2 * time.Second

	// Fase 4 dynamic config: refreshInterval is deliberately short (rather
	// than, say, a minute) so the plan's "behavior berubah tanpa restart
	// (dalam interval refresh)" validation step is observable in a
	// reasonable amount of time -- a template default, tune per deployment.
	dynamicConfigRefreshInterval = 5 * time.Second

	// Fase 4 reconciliation: pollInterval is how often the job runs;
	// stuckThreshold is how long a payment must have sat in "processing"
	// (since its last applied event) before the job will touch it at all.
	reconciliationPollInterval   = 15 * time.Second
	reconciliationStuckThreshold = 1 * time.Minute
)

func main() {
	if err := run_(); err != nil {
		slog.Error("worker exited with error", "error", err)
		os.Exit(1)
	}
}

func run_() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Load .env (repo root), if present, BEFORE bootstrap config below --
	// its values must be in the process environment before config.Load's
	// env var overlay step (and LoadDynamicSeed) run.
	config.LoadDotEnv()

	cfg, err := config.Load(configPath())
	if err != nil {
		return fmt.Errorf("load bootstrap config: %w", err)
	}

	// dynSeed's otel_enabled is read straight from config.toml because the
	// tracer below is created before Postgres connects -- see
	// config.DynamicSeed's doc. QueueDefaultName is also loaded here so it's
	// available as the fallback default even if the app_config row is
	// missing after Load below.
	dynSeed, err := config.LoadDynamicSeed(configPath())
	if err != nil {
		return fmt.Errorf("load dynamic config seed: %w", err)
	}

	obs := bootstrap.RegisterObservability(ctx, cfg, serviceName, dynSeed.OtelEnabled)
	logger := obs.Logger
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := obs.Tracer.Shutdown(shutdownCtx); err != nil {
			logger.Error("shutdown tracer provider", "error", err)
		}
	}()

	pool, err := db.NewPool(ctx, cfg.Postgres.DSN)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()
	logger.Info("connected to postgres")

	// Fase 4 dynamic config: loaded AFTER the Postgres connection above,
	// per the plan's fixed startup order. The initial Load is synchronous
	// and fails startup on error (there is no last-known-good snapshot at
	// t=0, consistent with db.NewPool already hard-failing on its own
	// Ping) -- every refresh AFTER this one falls back to the last-known-
	// good snapshot instead (see internal/config.Cache.Run).
	dynamicStore := config.NewDynamicStore(pool)
	dynamicCache := config.NewCache(dynamicStore, dynamicConfigRefreshInterval)
	if err := dynamicCache.Load(ctx); err != nil {
		return fmt.Errorf("load dynamic config: %w", err)
	}
	logger.Info("loaded dynamic config")

	// Reconcile the tracer (built pre-Postgres from config.toml's seed
	// value, see dynSeed above) against the live app_config row, now that
	// one can exist. One-shot -- see TracerManager.Reconcile's doc for why
	// this is not wired into dynamicCache's periodic refresh.
	obs.Tracer.Reconcile(ctx, dynamicCache.OtelEnabled(dynSeed.OtelEnabled))

	// Queue name: read ONCE here rather than via a live getter, and used for
	// both the asynq server's Queues (below) and the relay's producer side.
	// asynq.Server's queue set is fixed at construction -- if the relay
	// picked up a renamed queue live while the server kept consuming the
	// old one, tasks would enqueue into a queue nothing is listening on and
	// stall silently. Renaming the queue therefore requires a worker
	// restart; this snapshot is what makes producer and consumer agree for
	// this process's entire lifetime.
	queueName := dynamicCache.QueueName(dynSeed.QueueDefaultName)
	logger.Info("queue name resolved", "queue", queueName)

	// Domain wiring -- identical construction to cmd/server, plus the
	// worker-only pieces (webhook staging join, circuit breaker, bulkhead,
	// and now the Fase 4 provider-enabled check) wired via the With*
	// setters.
	providers := bootstrap.RegisterProviders()
	idemStore := idempotency.NewPGStore(pool)
	paymentRepo := payment.NewPGRepository(pool)
	webhookStore := webhook.NewStore(pool)
	breakers := circuitbreaker.NewRegistry(breakerFailureThreshold, breakerCooldown)
	bulkheadLimiter := bulkhead.New(bulkheadCapacityPerProv)

	paymentService := payment.NewService(paymentRepo, idemStore, providers).
		WithWebhookStaging(webhookStore).
		WithCircuitBreakers(breakers).
		WithBulkhead(bulkheadLimiter).
		WithProviderEnabledCheck(dynamicCache.ProviderEnabled)

	chargeHandler := func(ctx context.Context, payload []byte) error {
		var in payment.ChargeTaskInput
		if err := json.Unmarshal(payload, &in); err != nil {
			return fmt.Errorf("worker: unmarshal charge task payload: %w", err)
		}
		return paymentService.ProcessCharge(ctx, in)
	}

	refundHandler := func(ctx context.Context, payload []byte) error {
		var in payment.RefundTaskInput
		if err := json.Unmarshal(payload, &in); err != nil {
			return fmt.Errorf("worker: unmarshal refund task payload: %w", err)
		}
		return paymentService.ProcessRefund(ctx, in)
	}

	// Queue: durable asynq client + non-durable memory fallback, switching
	// on live Redis health (not just a boot-time check) -- validations 1
	// and 3.
	terminalFailures := queue.NewTerminalFailureRecorder(pool)

	memoryClient := queue.NewMemoryClient(terminalFailures.MemoryErrorRecorder())
	memoryClient.RegisterHandler(queue.TaskTypeCharge, func(ctx context.Context, _ string, payload []byte) error {
		return chargeHandler(ctx, payload)
	})
	memoryClient.RegisterHandler(queue.TaskTypeRefund, func(ctx context.Context, _ string, payload []byte) error {
		return refundHandler(ctx, payload)
	})

	asynqClient := queue.NewAsynqClient(cfg.Redis.Addr)
	redisHealth := queue.NewRedisHealthChecker(cfg.Redis.Addr, redisHealthInterval)
	switchingClient := queue.NewSwitchingClient(asynqClient, memoryClient, redisHealth)
	defer func() {
		if err := switchingClient.Close(); err != nil {
			logger.Error("close queue client", "error", err)
		}
	}()

	// Outbox relay: Postgres -> switchingClient. WithProviderEnabledCheck
	// wires the Fase 4 enqueue-time half of the provider-enabled check (the
	// other half, right before the actual provider call, lives inside
	// paymentService.ProcessCharge/ProcessRefund above).
	outboxStore := outbox.NewStore(pool)
	relay := outbox.NewRelay(outboxStore, switchingClient, relayBaseInterval, relayMaxInterval, relayBatchSize).
		WithProviderEnabledCheck(dynamicCache.ProviderEnabled).
		WithQueueName(func() string { return queueName })

	// asynq task server: the real (Redis-backed) consumption side. The
	// memory fallback's consumption side is memoryClient itself (it
	// invokes the handler directly on Enqueue) -- both paths share
	// chargeHandler/refundHandler, so behavior (including the no-retry
	// invariant) is identical regardless of which transport carried the
	// task.
	mux := asynq.NewServeMux()
	mux.HandleFunc(queue.TaskTypeCharge, func(ctx context.Context, task *asynq.Task) error {
		return chargeHandler(ctx, task.Payload())
	})
	mux.HandleFunc(queue.TaskTypeRefund, func(ctx context.Context, task *asynq.Task) error {
		return refundHandler(ctx, task.Payload())
	})

	// Fase 4 reconciliation job: polls payments stuck "processing" past
	// reconciliationStuckThreshold and sweeps staged webhook events with no
	// matching payment yet. See internal/reconciliation and
	// internal/domain/payment/reconcile.go.
	reconciler := reconciliation.New(paymentService, reconciliationPollInterval, reconciliationStuckThreshold)

	asynqServer := asynq.NewServer(
		asynq.RedisClientOpt{Addr: cfg.Redis.Addr},
		asynq.Config{
			Concurrency: asynqConcurrency,
			// Queues must list every queue name the relay might dispatch
			// into (here, just the one queueName snapshot above) -- asynq
			// defaults to consuming only "default" otherwise, which would
			// silently strand every task if queueName is anything else.
			Queues:       map[string]int{queueName: 1},
			ErrorHandler: terminalFailures.AsynqErrorHandler(),
		},
	)

	var g run.Group

	g.Add(run.SignalHandler(ctx, syscall.SIGINT, syscall.SIGTERM))

	relayExecute, relayInterrupt := lifecycle.RunnerActor(relay.Run)
	g.Add(relayExecute, relayInterrupt)

	asynqExecute, asynqInterrupt := lifecycle.AsynqServerActor(asynqServer, mux)
	g.Add(asynqExecute, asynqInterrupt)

	dynConfigExecute, dynConfigInterrupt := lifecycle.RunnerActor(dynamicCache.Run)
	g.Add(dynConfigExecute, dynConfigInterrupt)

	reconcileExecute, reconcileInterrupt := lifecycle.RunnerActor(reconciler.Run)
	g.Add(reconcileExecute, reconcileInterrupt)

	logger.Info("starting worker", "redis_addr", cfg.Redis.Addr)
	if err := g.Run(); err != nil && !errors.Is(err, run.ErrSignal) {
		return err
	}
	logger.Info("worker shutdown complete")
	return nil
}

func configPath() string {
	if v := os.Getenv("APP_CONFIG_FILE"); v != "" {
		return v
	}
	return "config.toml"
}
