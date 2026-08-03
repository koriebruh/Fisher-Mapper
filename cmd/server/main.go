// Command server bootstraps config, a Postgres pool, a Redis-backed asynq
// client, the PJP provider registry, the payment domain service, and a
// single fiber HTTP server exposing /healthz, /readyz, and (Phase 2)
// POST /payments — wired as oklog/run actors with a signal handler so
// shutdown is deterministic.
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
	"Fisher-Mapper/internal/transport/rest"
)

const serviceName = "fisher-mapper"

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
	// idempotency store, payment repository/service.
	providers := bootstrap.RegisterProviders()
	idemStore := idempotency.NewPGStore(pool)
	paymentRepo := payment.NewPGRepository(pool)
	paymentService := payment.NewService(paymentRepo, idemStore, providers)

	// 6. Transport: single fiber app with health + payment endpoints.
	app := rest.NewApp(rest.Deps{
		Pool:           pool,
		QueueClient:    queueClient,
		PaymentService: paymentService,
	})

	addr := fmt.Sprintf(":%d", cfg.HTTP.Port)

	var g run.Group

	g.Add(run.SignalHandler(ctx, syscall.SIGINT, syscall.SIGTERM))

	fiberExecute, fiberInterrupt := lifecycle.FiberActor(app, addr, 5*time.Second)
	g.Add(fiberExecute, fiberInterrupt)

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
