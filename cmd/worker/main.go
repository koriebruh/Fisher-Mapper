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
		// fx's default StopTimeout (15s) bounds the ENTIRE sequential OnStop
		// chain, not each hook individually. This binary's LIFO stop order
		// sums to ~23s worst case (asynq.Server's unconfigured 8s default +
		// the metrics HTTPServerActor's 5s + meterProvider.Shutdown's 5s +
		// tracer.Shutdown's 5s), which exceeds the default -- and fx aborts
		// any remaining OnStop hooks (e.g. pool.Close()) rather than just
		// running slow, unlike oklog/run.Group's interrupt() chain, which
		// had no such ceiling.
		fx.StopTimeout(60 * time.Second),
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
