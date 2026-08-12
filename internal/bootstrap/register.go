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
	"log/slog"
	"time"

	"Fisher-Mapper/internal/auth"
	"Fisher-Mapper/internal/config"
	"Fisher-Mapper/internal/observability"
	"Fisher-Mapper/internal/provider"
	"Fisher-Mapper/internal/provider/mock"
	"Fisher-Mapper/internal/secrets/env"
)

// Observability bundles what Register produces so main() can use the
// logger, reconcile the tracer against dynamic config once Postgres is up,
// and shut tracing down on exit.
type Observability struct {
	Logger *slog.Logger
	Tracer *observability.TracerManager
}

// RegisterObservability performs the Phase 1 observability registration, in
// order: (1) build the redacting slog.Logger and install it as
// slog.Default(); (2) build the tracer provider, gated by otelEnabled.
//
// otelEnabled comes from config.LoadDynamicSeed(config.toml), not
// Postgres app_config -- this call happens before any connection exists
// (see cmd/server/main.go's startup order), so the live dynamic-config row
// is not readable yet. Once it is, callers reconcile via
// Observability.Tracer.Reconcile. Never returns an error for the tracer
// half: a tracing failure is diagnostic, not something that should abort
// process startup (TracerManager falls back to a no-op provider on error
// internally).
func RegisterObservability(ctx context.Context, cfg config.Bootstrap, serviceName string, otelEnabled bool) Observability {
	logger := observability.NewLogger(cfg.Log.Level)
	slog.SetDefault(logger)

	tracer := observability.NewTracerManager(ctx, serviceName, otelEnabled)

	return Observability{Logger: logger, Tracer: tracer}
}

// RegisterProviders builds the PJP provider registry and registers every
// provider explicitly, by name, via provider.Registry.Register — never via
// `import _ "package"` for an init() side effect, per the project's
// no-blank-import rule. Mock is the only real provider in Phase 2 (plan:
// "PJP: mock-only dulu"); real PJPs register here the same way once
// implemented.
func RegisterProviders() *provider.Registry {
	registry := provider.NewRegistry()
	registry.Register("mock", mock.New(mock.Config{Name: "mock"}))
	return registry
}

// RegisterVerifiers builds the per-provider inbound webhook Verifier map,
// explicitly (no blank import), one entry per registered provider. dedup is
// wired into every Verifier so an already-applied provider_event_id is
// rejected before ever reaching state-transition logic (Fase 2 requirement,
// consumed here by the Fase 3 webhook route).
//
// Secret lookup goes through the secrets.Secrets interface (env impl) per
// the plan's "stub cheap" secrets-manager item, so swapping to a real
// secrets manager later never touches this call site. If
// PROVIDER_MOCK_SECRET is unset, "mock" is deliberately left OUT of the
// returned map rather than falling back to any fixed literal: a hardcoded
// fallback secret would let anyone who has read this source forge a valid
// webhook signature and push a payment to "succeeded". POST
// /webhooks/mock already fails closed (401 "no verifier configured") when a
// provider has no map entry -- see rest/webhook.go -- so omitting the entry
// is sufficient, no separate route-disabling logic needed.
func RegisterVerifiers(dedup auth.DedupChecker) map[string]auth.Verifier {
	secretsStore := env.New("")
	mockSecret := secretsStore.GetSecret("provider_mock_secret")
	if mockSecret == "" {
		slog.Warn("provider_mock_secret not configured; POST /webhooks/mock will reject every request (401) until it is set")
		return map[string]auth.Verifier{}
	}

	return map[string]auth.Verifier{
		"mock": &auth.HMACVerifier{
			Secret: mockSecret,
			Window: 5 * time.Minute,
			Dedup:  dedup,
		},
	}
}
