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

	cfg, err := config.Load(configPath())
	if err != nil {
		return fmt.Errorf("load bootstrap config: %w", err)
	}

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

	pool, err := db.NewPool(ctx, cfg.Postgres.DSN)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()
	logger.Info("connected to postgres")

	// Domain wiring -- identical construction to cmd/server, plus the
	// worker-only pieces (webhook staging join, circuit breaker, bulkhead)
	// wired via the With* setters.
	providers := bootstrap.RegisterProviders()
	idemStore := idempotency.NewPGStore(pool)
	paymentRepo := payment.NewPGRepository(pool)
	webhookStore := webhook.NewStore(pool)
	breakers := circuitbreaker.NewRegistry(breakerFailureThreshold, breakerCooldown)
	bulkheadLimiter := bulkhead.New(bulkheadCapacityPerProv)

	paymentService := payment.NewService(paymentRepo, idemStore, providers).
		WithWebhookStaging(webhookStore).
		WithCircuitBreakers(breakers).
		WithBulkhead(bulkheadLimiter)

	chargeHandler := func(ctx context.Context, payload []byte) error {
		var in payment.ChargeTaskInput
		if err := json.Unmarshal(payload, &in); err != nil {
			return fmt.Errorf("worker: unmarshal charge task payload: %w", err)
		}
		return paymentService.ProcessCharge(ctx, in)
	}

	// Queue: durable asynq client + non-durable memory fallback, switching
	// on live Redis health (not just a boot-time check) -- validations 1
	// and 3.
	terminalFailures := queue.NewTerminalFailureRecorder(pool)

	memoryClient := queue.NewMemoryClient(terminalFailures.MemoryErrorRecorder())
	memoryClient.RegisterHandler(queue.TaskTypeCharge, func(ctx context.Context, _ string, payload []byte) error {
		return chargeHandler(ctx, payload)
	})

	asynqClient := queue.NewAsynqClient(cfg.Redis.Addr)
	redisHealth := queue.NewRedisHealthChecker(cfg.Redis.Addr, redisHealthInterval)
	switchingClient := queue.NewSwitchingClient(asynqClient, memoryClient, redisHealth)
	defer func() {
		if err := switchingClient.Close(); err != nil {
			logger.Error("close queue client", "error", err)
		}
	}()

	// Outbox relay: Postgres -> switchingClient.
	outboxStore := outbox.NewStore(pool)
	relay := outbox.NewRelay(outboxStore, switchingClient, relayBaseInterval, relayMaxInterval, relayBatchSize)

	// asynq task server: the real (Redis-backed) consumption side. The
	// memory fallback's consumption side is memoryClient itself (it
	// invokes the handler directly on Enqueue) -- both paths share
	// chargeHandler, so behavior (including the no-retry invariant) is
	// identical regardless of which transport carried the task.
	mux := asynq.NewServeMux()
	mux.HandleFunc(queue.TaskTypeCharge, func(ctx context.Context, task *asynq.Task) error {
		return chargeHandler(ctx, task.Payload())
	})

	asynqServer := asynq.NewServer(
		asynq.RedisClientOpt{Addr: cfg.Redis.Addr},
		asynq.Config{
			Concurrency:  asynqConcurrency,
			ErrorHandler: terminalFailures.AsynqErrorHandler(),
		},
	)

	var g run.Group

	g.Add(run.SignalHandler(ctx, syscall.SIGINT, syscall.SIGTERM))

	relayExecute, relayInterrupt := lifecycle.RunnerActor(relay.Run)
	g.Add(relayExecute, relayInterrupt)

	asynqExecute, asynqInterrupt := lifecycle.AsynqServerActor(asynqServer, mux)
	g.Add(asynqExecute, asynqInterrupt)

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
	return "configs/config.toml"
}
