package observability

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"go.opentelemetry.io/otel/sdk/resource"
)

// SetupTracerProvider wires a real (not no-op) tracer provider backed by a
// stdout exporter and installs it as the global provider via
// otel.SetTracerProvider. This is intentionally the cheap option for Phase
// 1 — Phase 5 swaps the exporter for OTLP without touching call sites,
// since everything traces through otel.Tracer(...) / the global provider.
//
// Returned shutdown must be called during graceful shutdown to flush any
// buffered spans.
func SetupTracerProvider(ctx context.Context, serviceName string) (shutdown func(context.Context) error, err error) {
	exporter, err := stdouttrace.New(stdouttrace.WithoutTimestamps())
	if err != nil {
		return nil, fmt.Errorf("observability: create stdout trace exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			attribute.String("service.name", serviceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("observability: build resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)

	return tp.Shutdown, nil
}
