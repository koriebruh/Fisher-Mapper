package observability

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"

	"go.opentelemetry.io/otel/attribute"
)

// NewMeterProvider builds an OTel MeterProvider backed by a Prometheus
// exporter (pull-based: the SDK computes the current value of every
// instrument only when something scrapes the registry below, so idle
// processes pay no export cost) -- chosen over a stdout push exporter
// because a template project benefits far more from "curl /metrics and see
// real numbers" than from periodic JSON blobs in the log stream, and
// Prometheus's text format is what most operators' existing alerting stacks
// already scrape.
//
// A dedicated prometheus.Registry (not prometheus.DefaultRegisterer) is used
// deliberately: the default registerer is process-global mutable state, and
// this constructor may run more than once in a single process across tests.
// registry is returned alongside the provider so the caller can build a
// promhttp handler from it.
func NewMeterProvider(ctx context.Context, serviceName string) (*sdkmetric.MeterProvider, *prometheus.Registry, error) {
	registry := prometheus.NewRegistry()

	exporter, err := otelprom.New(otelprom.WithRegisterer(registry))
	if err != nil {
		return nil, nil, fmt.Errorf("observability: build prometheus exporter: %w", err)
	}

	res, err := resource.New(ctx, resource.WithAttributes(attribute.String("service.name", serviceName)))
	if err != nil {
		return nil, nil, fmt.Errorf("observability: build resource: %w", err)
	}

	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter), sdkmetric.WithResource(res))
	otel.SetMeterProvider(mp)
	return mp, registry, nil
}

// Metrics holds every instrument this phase emits, per the plan's list:
// HTTP request latency, outbox dispatch lag, DB pool stats, DLQ/terminal-
// failures depth, and reconciliation mismatch count. Built once per process
// from the MeterProvider above and threaded through to whichever call site
// (REST middleware, outbox relay, poller, domain reconcile hook) records
// into it.
type Metrics struct {
	HTTPRequestDuration    metric.Float64Histogram
	OutboxDispatchLag      metric.Float64Histogram
	DBPoolAcquiredConns    metric.Int64Gauge
	DBPoolIdleConns        metric.Int64Gauge
	DBPoolTotalConns       metric.Int64Gauge
	TerminalFailuresTotal  metric.Int64Gauge
	ReconciliationMismatch metric.Int64Counter
}

// NewMetrics creates every instrument against meter. Returns an error (never
// panics) if instrument creation fails -- callers should treat that exactly
// like TracerManager treats a broken tracer: log and fall back to not
// recording metrics, never abort process startup over an observability
// dependency.
func NewMetrics(meter metric.Meter) (*Metrics, error) {
	// Explicit bucket boundaries, in seconds: the OTel SDK's DEFAULT
	// boundaries (0, 5, 10, 25, 50, ...) are tuned for millisecond-unit
	// histograms, so with unit "s" every real HTTP request or outbox
	// dispatch (sub-second to low-seconds) collapses into the first
	// nonzero bucket -- histogram_quantile in the alert doc would compute
	// "somewhere between 0 and 5 seconds" for every p99, which is exactly
	// the "threshold without a real metric behind it" failure the plan's
	// Fase 5 item warns about. These boundaries bracket the alert
	// thresholds in docs/observability-alerts.md (2s and 30s) so those
	// queries actually have resolution near the values operators alert on.
	httpDuration, err := meter.Float64Histogram(
		"fisher_http_request_duration_seconds",
		metric.WithDescription("HTTP request latency by route and status code"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 2.5, 5, 10, 30),
	)
	if err != nil {
		return nil, fmt.Errorf("observability: build http_request_duration histogram: %w", err)
	}

	outboxLag, err := meter.Float64Histogram(
		"fisher_outbox_dispatch_lag_seconds",
		metric.WithDescription("time between an outbox row's created_at and the moment it is successfully handed to the queue client"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300),
	)
	if err != nil {
		return nil, fmt.Errorf("observability: build outbox_dispatch_lag histogram: %w", err)
	}

	dbAcquired, err := meter.Int64Gauge(
		"fisher_db_pool_acquired_conns",
		metric.WithDescription("pgxpool connections currently acquired (in use)"),
	)
	if err != nil {
		return nil, fmt.Errorf("observability: build db_pool_acquired_conns gauge: %w", err)
	}

	dbIdle, err := meter.Int64Gauge(
		"fisher_db_pool_idle_conns",
		metric.WithDescription("pgxpool connections currently idle"),
	)
	if err != nil {
		return nil, fmt.Errorf("observability: build db_pool_idle_conns gauge: %w", err)
	}

	dbTotal, err := meter.Int64Gauge(
		"fisher_db_pool_total_conns",
		metric.WithDescription("pgxpool connections currently open (acquired + idle + constructing)"),
	)
	if err != nil {
		return nil, fmt.Errorf("observability: build db_pool_total_conns gauge: %w", err)
	}

	// Named _total (not _depth): terminal_failures is append-only (no
	// resolution/ack column), so this value only ever climbs -- it is the
	// cumulative count of terminal failures ever recorded, not a live queue
	// depth. See docs/observability-alerts.md for why the alert on this
	// metric is a rate/delta check, not a bare ">0".
	dlqTotal, err := meter.Int64Gauge(
		"fisher_terminal_failures_total",
		metric.WithDescription("cumulative count of rows in terminal_failures (append-only DLQ inspection table)"),
	)
	if err != nil {
		return nil, fmt.Errorf("observability: build terminal_failures_total gauge: %w", err)
	}

	mismatch, err := meter.Int64Counter(
		"fisher_reconciliation_mismatch_total",
		metric.WithDescription("count of reconciliation passes where a provider's GetStatus amount/currency did not match the stored payment (never auto-applied -- see payment.ReconcilePayment)"),
	)
	if err != nil {
		return nil, fmt.Errorf("observability: build reconciliation_mismatch_total counter: %w", err)
	}

	return &Metrics{
		HTTPRequestDuration:    httpDuration,
		OutboxDispatchLag:      outboxLag,
		DBPoolAcquiredConns:    dbAcquired,
		DBPoolIdleConns:        dbIdle,
		DBPoolTotalConns:       dbTotal,
		TerminalFailuresTotal:  dlqTotal,
		ReconciliationMismatch: mismatch,
	}, nil
}
