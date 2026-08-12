package observability

import (
	"context"
	"log/slog"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"go.opentelemetry.io/otel/sdk/resource"
)

// TracerManager owns whichever tracer provider is currently installed as
// otel's global provider, so it can be swapped later (Reconcile) without
// touching any of the app's otel.Tracer(...) call sites: otel's global
// provider is a proxy that resolves to whatever is registered at span-start
// time, not at Tracer()-call time, so re-registering here is enough.
//
// This exists because of an ordering constraint: the tracer must be created
// before Postgres connects (see bootstrap.RegisterObservability), so the
// very first decision of enabled/disabled can only come from config.toml
// (config.LoadDynamicSeed), not the live app_config row. Reconcile is the
// one-shot correction applied once Postgres/dynamic-config becomes
// available in cmd/server and cmd/worker.
type TracerManager struct {
	serviceName string

	mu       sync.Mutex
	enabled  bool
	shutdown func(context.Context) error
}

// NewTracerManager installs either a real stdout-exporter tracer provider
// (enabled=true) or otel's no-op provider (enabled=false), and returns the
// manager for later Shutdown/Reconcile calls. Never fails: if building the
// real provider errors (exporter/resource construction), it logs a warning
// and installs the no-op provider instead -- tracing is diagnostic, not
// load-bearing, so it must never abort process startup.
func NewTracerManager(ctx context.Context, serviceName string, enabled bool) *TracerManager {
	tm := &TracerManager{serviceName: serviceName}
	tm.install(ctx, enabled)
	return tm
}

func (tm *TracerManager) install(ctx context.Context, enabled bool) {
	if !enabled {
		otel.SetTracerProvider(noop.NewTracerProvider())
		tm.enabled = false
		tm.shutdown = func(context.Context) error { return nil }
		return
	}

	tp, err := buildStdoutTracerProvider(ctx, tm.serviceName)
	if err != nil {
		slog.Warn("observability: failed to build tracer provider, falling back to no-op", "error", err)
		otel.SetTracerProvider(noop.NewTracerProvider())
		tm.enabled = false
		tm.shutdown = func(context.Context) error { return nil }
		return
	}

	otel.SetTracerProvider(tp)
	tm.enabled = true
	tm.shutdown = tp.Shutdown
}

// buildStdoutTracerProvider is the Phase 1 cheap real exporter -- Phase 5
// swaps it for OTLP without touching call sites, since everything traces
// through the global provider.
func buildStdoutTracerProvider(ctx context.Context, serviceName string) (*sdktrace.TracerProvider, error) {
	exporter, err := stdouttrace.New(stdouttrace.WithoutTimestamps())
	if err != nil {
		return nil, err
	}
	res, err := resource.New(ctx, resource.WithAttributes(attribute.String("service.name", serviceName)))
	if err != nil {
		return nil, err
	}
	return sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithResource(res)), nil
}

// Reconcile swaps the installed provider if enabled differs from what is
// currently installed, shutting down the superseded provider. Call once per
// process right after dynamic config becomes available (not from the
// periodic Cache refresh loop -- repeated tracer teardown/rebuild on every
// refresh tick is an untested path this template does not need).
func (tm *TracerManager) Reconcile(ctx context.Context, enabled bool) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if enabled == tm.enabled {
		return
	}
	prevShutdown := tm.shutdown
	tm.install(ctx, enabled)
	if prevShutdown != nil {
		_ = prevShutdown(ctx)
	}
}

// Shutdown flushes/stops whichever provider is currently installed.
func (tm *TracerManager) Shutdown(ctx context.Context) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if tm.shutdown == nil {
		return nil
	}
	return tm.shutdown(ctx)
}
