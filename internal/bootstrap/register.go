// Package bootstrap holds explicit, order-sensitive registration calls.
//
// Per the project's "no blank import" rule: provider/driver/exporter
// registration that is sensitive to ordering is done by calling exported
// functions here, from main(), in a fixed sequence — never via
// `import _ "package"` for its init() side effect. init() order between
// packages is not guaranteed by the language, which makes blank-import
// registration a source of hard-to-diagnose startup bugs (e.g. a
// connection opened before its logger/tracer exists).
package bootstrap

import (
	"context"
	"fmt"
	"log/slog"

	"Fisher-Mapper/internal/config"
	"Fisher-Mapper/internal/observability"
)

// Observability bundles what Register produces so main() can use the
// logger and shut tracing down on exit.
type Observability struct {
	Logger         *slog.Logger
	ShutdownTracer func(context.Context) error
}

// RegisterObservability performs the Phase 1 observability registration,
// in order:
//  1. build the redacting slog.Logger and install it as slog.Default()
//  2. build the OTel tracer provider (stdout exporter) and install it as
//     the global tracer provider
//
// This must run before any other actor (fiber server, DB pool, queue
// client) starts, so every subsequent log line and span goes through the
// configured logger/provider rather than the library defaults.
func RegisterObservability(ctx context.Context, cfg config.Bootstrap, serviceName string) (Observability, error) {
	logger := observability.NewLogger(cfg.Log.Level)
	slog.SetDefault(logger)

	shutdownTracer, err := observability.SetupTracerProvider(ctx, serviceName)
	if err != nil {
		return Observability{}, fmt.Errorf("bootstrap: register tracer provider: %w", err)
	}

	return Observability{
		Logger:         logger,
		ShutdownTracer: shutdownTracer,
	}, nil
}
