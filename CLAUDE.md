# Fisher-Mapper

Go payment-gateway template: HTTP (fiber) + gRPC transport in front of a shared
payment domain service, Postgres for state, Redis/asynq for async task
dispatch. Module name is `Fisher-Mapper` (see `go.mod`), Go 1.26.6.

## Binaries (`cmd/`)

Three separate binaries, each with its own `main.go`, sharing everything else
via `internal/`:

- **`cmd/server`** — HTTP-only. Exposes `/healthz`, `/readyz`, payment CRUD,
  `/webhooks/{provider}`, `/admin/config`, plus a second gRPC listener for the
  same payment operations. Never calls a payment provider directly or touches
  Redis on the request path — `create-payment` only writes Postgres (payment
  row + outbox row, one transaction).
- **`cmd/worker`** — the only process that calls a provider's
  Charge/Authorize/Refund/Payout. Runs the outbox relay (Postgres → queue),
  the asynq task server (queue → provider), and the reconciliation job. Has
  its own circuit breaker, bulkhead, and a memory-fallback queue that kicks in
  when Redis is unreachable.
- **`cmd/migrate`** — applies goose migrations (`internal/platform/db/migrations`).
  Also has `-create-tenant-key` to mint a `tenant_api_keys` row (prints the
  plaintext key once to stdout — that's the only retrieval channel, not a bug).
  Deliberately separate from `cmd/server` — schema migration is an operator
  action, never triggered implicitly on boot.

Startup order is fixed and documented at the top of each `main.go`: config →
observability (logger + tracer) → Postgres → Redis → domain wiring →
transport → run actors. Both `cmd/server` and `cmd/worker` are being migrated
from manual `oklog/run.Group` wiring to `go.uber.org/fx` — see **In-flight
work** below before touching either `main.go`.

## Layout (`internal/`)

- `domain/payment` — the payment `Service` and its `With*` optional
  dependencies (circuit breaker, bulkhead, webhook staging, callback
  notifier, reconciliation hooks). `domain/apperror` — transport-agnostic
  error taxonomy (`Code` enum); transports map `Code` to HTTP/gRPC status at
  the edge, never the other way around.
- `messaging/` — `outbox` (Postgres→queue relay), `idempotency`,
  `reconciliation`, `webhook` (inbound staging).
- `platform/` — `config` (bootstrap TOML+env, plus the Postgres-backed
  `DynamicStore`/`Cache` for live-toggleable flags), `db`, `observability`
  (logger, OTel tracer/meter, Prometheus metrics), `queue` (asynq client +
  memory fallback + health-based switching), `bootstrap` (explicit,
  order-sensitive provider/verifier registration — no blank imports for
  init() side effects, ever), `lifecycle` (actor adapters: `(execute,
  interrupt)` pairs for run-group-style wiring), `tenantauth`, `secrets`.
- `provider/` — the PJP registry and provider interface; `mock` is the only
  real provider today.
- `resilience/` — `ratelimit`, `circuitbreaker`, `bulkhead`, `ssrf` (the
  outbound-webhook SSRF-safe dialer).
- `transport/rest` and `transport/grpc` — thin; both share one
  `payment.Service` instance, never re-implement domain logic.

## Config

Two tiers, deliberately kept separate:
- **Bootstrap** (`config.toml` + env overrides, `internal/platform/config.Bootstrap`) —
  process tuning read once at startup (ports, pool sizes, timeouts, rate
  limit rate/burst). Never touched by `/admin/config`.
- **Dynamic** (`internal/platform/config.DynamicStore`/`Cache`, backed by a
  Postgres `app_config` table) — feature-flag-shaped toggles
  (`otel_enabled`, `ratelimit.enabled`, per-provider enabled, circuit-breaker
  enabled, queue name) that `/admin/config` can flip live, refreshed
  periodically by a background actor. `config.DynamicSeed` (read straight
  from `config.toml`) is the pre-Postgres fallback used only before the
  first successful `Cache.Load`.

## Conventions

- **Comments: WHY only, never WHAT.** Well-named identifiers already say
  what code does. A comment earns its place only for a hidden constraint, an
  ordering requirement, a workaround, or something that would surprise a
  reader. This is enforced by convention, not just linted — see the existing
  comment density in `internal/platform/config/dynamic.go` or any
  `cmd/*/main.go` for the target style. `golangci-lint`'s stock
  exported-doc-comment rule is deliberately disabled for exactly this
  reason (see `.golangci.yml`'s `exclusions` comment).
- **No blank imports for side effects.** Provider/driver/verifier
  registration goes through explicit, ordered function calls in
  `internal/platform/bootstrap`, called from `main()` — `init()` ordering
  between packages isn't guaranteed by the language.
- **Errors:** wrap with `%w` and compare with `errors.Is`/`errors.As`, never
  string-match or `==`-compare error values (`errorlint`/`nilerr` enforce
  this in CI). Domain-layer errors go through `apperror.Code`.
- **"Degraded, not dead" startup.** A failed Redis ping, a failed
  meter-provider build, or a failed initial dynamic-config load in
  `cmd/server` all log a warning and continue — see each `main.go`'s
  comments for exactly which failures are fatal (Postgres connect always is)
  versus which degrade gracefully.
- Every exported struct/func that isn't obvious from its name has a doc
  comment explaining the *why*; don't add a doc comment that just restates
  the name.

## Build / test / lint

```
make build          # bin/server, bin/worker, bin/migrate
make run             # build + run cmd/server
make run-worker      # build + run cmd/worker
make migrate-up      # build + run cmd/migrate
make docker-up       # docker compose up -d (postgres, redis, ...)
make proto           # regenerate internal/transport/grpc/pb/ via buf (pinned tool versions, see Makefile)

go test ./...                    # unit tests
go test ./... -race -count=1     # what pre-push actually runs
golangci-lint run                # full-repo lint (CI's authority)
golangci-lint run --new-from-rev=HEAD   # what pre-commit runs, changed files only
```

`lefthook install` wires the above into git hooks (pre-commit: gofmt +
go vet + changed-files lint; pre-push: build + race tests + `go mod tidy`
drift check). CI (`.github/workflows/ci.yml`, `codeql.yml`) mirrors the same
checks — passing locally should mean passing in CI.

## In-flight work

`main` is mid-migration from manual `oklog/run.Group` wiring to
`go.uber.org/fx` (dig-based DI) across all three `cmd/` binaries. Plan and
full implementation ledger:

- Plan: `docs/superpowers/plans/2026-08-21-fx-dig-dependency-injection.md`
- Work is happening on branch `feature/fx-dig-di` (a git worktree at
  `../Fisher-Mapper-fx-dig`), driven task-by-task via
  `superpowers:subagent-driven-development`. Progress/rulings ledger:
  `.superpowers/sdd/2026-08-21-fx-dig-dependency-injection/progress.md`
  inside that worktree.
- Key gotcha already hit twice during this migration, worth knowing before
  writing more fx code anywhere in this repo: **`fx.App` bounds `Start()`/
  `Stop()` themselves with an internal timeout (`fx.DefaultTimeout`, 15s),
  independent of what `ctx` a hook's own code uses internally.** Real,
  possibly-long-running work must never run synchronously inside an
  `fx.Lifecycle.OnStart`/`OnStop` hook or an `fx.Invoke` executed during that
  window — see `internal/platform/fxbridge.Bridge` (launches a goroutine and
  returns immediately) for the pattern used everywhere in `cmd/server`/
  `cmd/worker`, and `cmd/migrate/main.go` (runs its actual work as a plain
  statement between manual `app.Start()`/`app.Stop()` calls) for the
  one-shot-CLI variant.
