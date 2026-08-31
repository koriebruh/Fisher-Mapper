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
	"time"

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
		// fx's default StopTimeout (15s) bounds the ENTIRE sequential OnStop
		// chain, not each hook individually. This binary's LIFO stop order
		// sums to ~15s worst case (fiber's and gRPC's bounded shutdown
		// timeouts + dynamic-config watcher + metrics poller + meterProvider
		// .Shutdown's 5s + tracer.Shutdown's 5s), which can exceed the default
		// -- and fx aborts any remaining OnStop hooks (e.g. pool.Close())
		// rather than just running slow, unlike oklog/run.Group's interrupt()
		// chain, which had no such ceiling.
		fx.StopTimeout(60*time.Second),
	)

	if err := app.Err(); err != nil {
		slog.Error("[server] main: dependency graph failed to build", "component", "server", "error", err)
		os.Exit(1)
	}
	app.Run()
}
