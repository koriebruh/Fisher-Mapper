# Observability alert thresholds (Fase 5)

This is guidance for whatever alerting stack an operator points at this
template's `/metrics` endpoints (`cmd/server`'s, on `cfg.HTTP.Port`; and
`cmd/worker`'s, on `APP_WORKER_METRICS_PORT` / default `9101`). No alerting
system is built here -- no Alertmanager config, no PagerDuty integration --
this is only the threshold guidance a future operator configures into
whatever they use. Every metric named below is emitted today (see
`internal/platform/observability/metrics.go`); this document does not
invent a threshold for anything that isn't actually instrumented.

Per this project's money-handling stance, thresholds on payment-integrity
signals (reconciliation mismatch, DLQ/terminal-failure growth) are
deliberately conservative -- paranoid, even -- because the failure modes
behind them are exactly the double-charge / lost-money scenarios the rest of
the plan spent so much effort avoiding. A false-positive page here costs an
operator a few minutes; a missed one costs a customer their money.

## `fisher_reconciliation_mismatch_total` (counter, cmd/worker)

Incremented every time `payment.ReconcilePayment` sees a provider's
`GetStatus` response whose amount/currency does NOT match the stored
payment (see `internal/domain/payment/reconcile.go`) -- the payment is
deliberately left `processing`, never auto-marked succeeded/failed, exactly
so this can never silently move money.

- **Alert: `increase(fisher_reconciliation_mismatch_total[5m]) > 0` -> page immediately.**
  This should never happen in normal operation. Any increase is either a
  provider data-integrity bug or an attempted amount-tampering webhook/
  response -- both warrant a human looking at the specific payment_id in the
  log line (`reconciliation mismatch: ...`) before anything auto-resolves
  it. Do not wait for a volume threshold; the first occurrence is the
  signal.

## `fisher_terminal_failures_total` (gauge, cmd/worker)

Cumulative count of rows in `terminal_failures` (the DLQ inspection table).
**This is a monotonically non-decreasing total, not a live queue depth** --
`terminal_failures` is append-only with no resolution/ack column, so a bare
`> 0` threshold fires forever after the first-ever incident and is useless
in practice. Alert on the *rate of new rows*, not the absolute value:

- **Alert: `increase(fisher_terminal_failures_total[15m]) > 0` -> page, a payment/refund needs manual review.**
  Every row here is, by construction (see `queue.TerminalFailureRecorder`'s
  doc), a genuinely unexpected failure (unregistered provider, malformed
  payload, a DB write failing) -- never the expected "charge timed out,
  payment stays processing" outcome, which is reported as a handler success
  specifically so it never lands here. A new row is always actionable:
  inspect the row's `payload`/`error` columns and either fix the underlying
  cause and replay, or document why it's unrecoverable.
- If an operator wants a standing "how many unresolved incidents exist"
  view rather than a rate, track `fisher_terminal_failures_total` against a
  separately operator-maintained baseline (e.g. its value at last on-call
  handoff), not against zero.

## `fisher_outbox_dispatch_lag_seconds` (histogram, cmd/worker, `task_type` label)

Time between an outbox row's `created_at` and the moment the relay
successfully hands it to the queue client (asynq or the memory fallback).

- **Alert: `histogram_quantile(0.99, rate(fisher_outbox_dispatch_lag_seconds_bucket[5m])) > 30` -> investigate relay/Redis health.**
  30s is a template default matching the relay's own backoff ceiling
  (`relayMaxInterval` in `cmd/worker/main.go`) -- p99 lag consistently above
  that means the relay is backed off (Redis flaky or down, Postgres slow
  under `FOR UPDATE SKIP LOCKED` contention) rather than just briefly
  degraded. Tune per deployment's actual relay poll interval if changed.
- **Secondary: `rate(fisher_outbox_dispatch_lag_seconds_count[5m]) == 0` while payments are being created -> the relay may be stuck/crashed**,
  not just slow -- no dispatches recorded at all is a different failure
  mode than "recorded but slow", and this metric alone can't distinguish
  them (a stuck relay never gets to the point of recording anything).
  Pair with a liveness check on the worker process itself.

## `fisher_db_pool_acquired_conns` / `_idle_conns` / `_total_conns` (gauges, both processes)

Polled periodically (`internal/platform/observability.Poller`) from each
process's own `pgxpool.Pool.Stat()`.

- **Alert: `fisher_db_pool_acquired_conns / fisher_db_pool_total_conns > 0.9` sustained for 5m -> pool exhaustion risk.**
  Sustained near-100% utilization means requests/tasks are about to start
  queuing on `Acquire` (or timing out) -- investigate slow queries or a
  connection leak before it becomes an outage, not after.
- **Alert: `fisher_db_pool_total_conns == 0` while the process is otherwise healthy (`/healthz` 200) -> pool misconfiguration or Postgres unreachable**,
  worth a page since `/readyz` should already be catching this, but a gap
  here (metric says 0 conns, readyz says fine) is itself worth investigating
  as a monitoring bug.

## `fisher_http_request_duration_seconds` (histogram, cmd/server, `method`/`route`/`status_code` labels)

- **Alert: `histogram_quantile(0.99, rate(fisher_http_request_duration_seconds_bucket{route="/payments"}[5m])) > 2` -> investigate.**
  This is a latency/availability signal, not a money-integrity one -- less
  paranoid than the sections above. A slow `POST /payments` is a user
  experience problem (the endpoint is async by design -- see
  `payment.Service.CreatePayment`'s doc -- so 2s for an outbox-insert-only
  request is already generous); it does not by itself indicate a
  double-charge or data-integrity risk the way the sections above do.
- **Alert: `rate(fisher_http_request_duration_seconds_count{status_code=~"5.."}[5m]) > 0` -> investigate error rate**,
  standard practice, included for completeness -- not specific to this
  project's money-handling concerns.

## What is deliberately NOT covered here

- **Queue depth** (asynq's own pending-task count): not instrumented in
  this phase (the plan's metric list covers outbox lag, which is the
  pre-queue half of the same backlog signal; asynq's own Redis-side depth
  would need `asynq.Inspector`, deferred as it's not on the plan's list).
- **gRPC-side metrics**: Fase 6 (gRPC transport) does not exist yet -- see
  the Fase 5 phase report for confirmation that the tracer provider is
  otelgrpc-ready (global provider, standard SDK) so that phase can add
  `otelgrpc.NewServerHandler()` with no changes to this phase's code.
