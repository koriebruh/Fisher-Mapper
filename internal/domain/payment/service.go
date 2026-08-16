package payment

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"Fisher-Mapper/internal/domain/apperror"
	"Fisher-Mapper/internal/messaging/idempotency"
	"Fisher-Mapper/internal/messaging/outbox"
	"Fisher-Mapper/internal/messaging/webhook"
	"Fisher-Mapper/internal/platform/queue"
	"Fisher-Mapper/internal/provider"
	"Fisher-Mapper/internal/resilience/bulkhead"
	"Fisher-Mapper/internal/resilience/circuitbreaker"
)

// CreatePaymentInput is the domain-level input for Service.CreatePayment,
// decoded from the transport-layer request body by the caller.
type CreatePaymentInput struct {
	TenantID      string
	Livemode      bool
	Currency      string
	Amount        int64
	Provider      string
	PaymentMethod string
	Metadata      map[string]string

	// CallbackURL is an optional caller-supplied best-effort delivery
	// target, validated by ValidateCallbackURL below and notified once this
	// payment reaches a terminal status (see Service.WithCallbackNotifier).
	// Nil when the caller didn't set one.
	CallbackURL *string

	// Envelope is populated entirely by the TRANSPORT layer (REST/gRPC),
	// never derived here -- see Envelope's own doc for which sub-fields are
	// caller-supplied vs transport-set. RequestIP/RequestUserAgent/TraceID
	// deliberately do NOT feed into the gRPC transport's idempotency
	// fingerprint (see grpc.fingerprintBasis callers) -- they vary per
	// attempt/peer, and a fingerprint that shifted with them would turn a
	// legitimate retry from a different network path into a false
	// CodeIdempotencyConflict.
	Envelope
}

// CreatePaymentOutput is what CreatePayment returns, and — serialized
// verbatim minus Replayed — what gets stored in the idempotency record for
// later replay.
//
// Fase 3 addendum: CreatePayment no longer calls the provider synchronously,
// so Status here is always StatusPending on a fresh (non-replayed) call —
// this is an acknowledgment that the charge has been accepted and queued,
// not its final outcome. Callers that need the eventual result poll
// GET /payments/{id} (or, in tests, call Service.ProcessCharge directly and
// re-fetch the payment row) rather than reading it out of this response.
type CreatePaymentOutput struct {
	PaymentID   uuid.UUID `json:"payment_id"`
	Status      Status    `json:"status"`
	ProviderRef string    `json:"provider_ref,omitempty"`

	// Replayed is true when this response was served from a stored
	// idempotency record rather than freshly computed. Never persisted
	// (json:"-") — it depends on which call produced the value, not on
	// the stored data itself.
	Replayed bool `json:"-"`
}

// providerRegistry is the subset of *provider.Registry Service depends on,
// declared as an interface so tests can stub it without a real registry.
type providerRegistry interface {
	Get(name string) (provider.Provider, error)
}

// Service is the payment domain service: the single place business logic
// lives, called by thin REST (and later gRPC) handlers, and by the async
// charge worker (Fase 3).
type Service struct {
	repo      Repository
	idem      idempotency.Store
	providers providerRegistry
	now       func() time.Time

	// inProgressPollInterval/Timeout bound how long a concurrent request
	// that lost the idempotency-key race waits for the winner to finish
	// before giving up and reporting "in progress" rather than blocking
	// indefinitely.
	inProgressPollInterval time.Duration
	inProgressPollTimeout  time.Duration

	// staging, breakers, and bulkheadLimiter are all optional (nil-safe):
	// set via the With* methods below by whichever process actually needs
	// them (the worker), so cmd/server's HTTP-only Service instance can
	// leave them unset without any behavior change. Kept as separate
	// setters rather than NewService parameters so existing call sites
	// (Fase 2 tests, cmd/server) don't need to change.
	staging         *webhook.Store
	breakers        *circuitbreaker.Registry
	bulkheadLimiter *bulkhead.Limiter

	// providerEnabled is the Fase 4 dynamic-config provider-enabled check,
	// injected as a plain func so this package never imports internal/platform/config
	// (which owns the cache/refresh machinery) -- keeps the domain layer's
	// dependency graph exactly as narrow as it was before Fase 4. nil means
	// "always enabled" (backward compatible: cmd/server's HTTP-only Service
	// never sets this, and does not need to -- it never calls a provider).
	providerEnabled func(providerName string) bool

	// circuitBreakerEnabled is the dynamic-config circuitbreaker.enabled
	// check, injected the same plain-func way as providerEnabled. nil (or a
	// func returning true) means "breaker checks apply as normal"; false
	// makes breakerFor report no breaker at all, so ProcessCharge/
	// ProcessRefund reach the provider regardless of trip state -- useful
	// against a sandbox PJP that intentionally simulates failures, where a
	// tripped breaker would just get in the way of testing.
	circuitBreakerEnabled func() bool

	// onReconciliationMismatch is the Fase 5 reconciliation-mismatch-count
	// metric hook, injected the same way (a plain func, not an
	// internal/platform/observability import) for the identical reason: the
	// domain layer stays decoupled from the metrics/observability stack.
	// Takes ctx (unlike providerEnabled) so the metric recording at the call
	// site can eventually carry an exemplar linking it back to the
	// reconciliation pass's trace, once one exists. nil means "no metric
	// wired" (e.g. cmd/server's HTTP-only Service, which never reconciles
	// anything).
	onReconciliationMismatch func(ctx context.Context)

	// callbackNotifier is the Step 6 best-effort delivery hook, injected the
	// same plain-func way as providerEnabled/onReconciliationMismatch so
	// this package never imports net/http directly for it -- see
	// CallbackNotifier's doc (callback_notify.go) for the delivery contract
	// (fire-and-forget, must never block/panic/propagate an error). nil
	// means "no callback delivery wired" (e.g. cmd/server's HTTP-only
	// Service, and any test that doesn't care about this path).
	callbackNotifier CallbackNotifier
}

func NewService(repo Repository, idem idempotency.Store, providers providerRegistry) *Service {
	return &Service{
		repo:                   repo,
		idem:                   idem,
		providers:              providers,
		now:                    func() time.Time { return time.Now().UTC() },
		inProgressPollInterval: 25 * time.Millisecond,
		inProgressPollTimeout:  2 * time.Second,
	}
}

// WithWebhookStaging enables ProcessCharge's post-charge webhook join
// (applying any staged event that arrived before this payment existed —
// plan item 9). Returns s for chaining at construction time.
func (s *Service) WithWebhookStaging(store *webhook.Store) *Service {
	s.staging = store
	return s
}

// WithCircuitBreakers enables a per-provider circuit breaker check before
// ProcessCharge calls the provider. Returns s for chaining.
func (s *Service) WithCircuitBreakers(registry *circuitbreaker.Registry) *Service {
	s.breakers = registry
	return s
}

// WithBulkhead enables a per-provider concurrency limiter around
// ProcessCharge's provider call, so one slow provider can't starve workers
// meant to process other providers' charges. Returns s for chaining.
func (s *Service) WithBulkhead(limiter *bulkhead.Limiter) *Service {
	s.bulkheadLimiter = limiter
	return s
}

// WithProviderEnabledCheck wires the Fase 4 dynamic-config provider-enabled
// flag check into ProcessCharge/ProcessRefund. fn is called with a cache
// read only (per plan: "idempotency yang cegah double-charge bukan flag
// freshness" -- this check is a best-effort skip, not a strict gate; no live
// DB round-trip happens per payment/refund). Returns s for chaining.
func (s *Service) WithProviderEnabledCheck(fn func(providerName string) bool) *Service {
	s.providerEnabled = fn
	return s
}

// WithCircuitBreakerEnabledCheck wires the dynamic-config
// circuitbreaker.enabled flag check into breakerFor (see its doc). Returns
// s for chaining.
func (s *Service) WithCircuitBreakerEnabledCheck(fn func() bool) *Service {
	s.circuitBreakerEnabled = fn
	return s
}

// WithReconciliationMismatchHook wires the Fase 5 metric hook fn, called
// once per detected amount/currency mismatch inside ReconcilePayment (see
// reconcile.go). Returns s for chaining.
func (s *Service) WithReconciliationMismatchHook(fn func(ctx context.Context)) *Service {
	s.onReconciliationMismatch = fn
	return s
}

// WithCallbackNotifier wires the Step 6 best-effort callback delivery hook
// -- see CallbackNotifier's doc (callback_notify.go). Returns s for
// chaining.
func (s *Service) WithCallbackNotifier(fn CallbackNotifier) *Service {
	s.callbackNotifier = fn
	return s
}

// isProviderEnabled reports whether providerName is enabled, treating a
// nil providerEnabled (not wired -- e.g. cmd/server's HTTP-only Service) as
// always-enabled.
func (s *Service) isProviderEnabled(providerName string) bool {
	if s.providerEnabled == nil {
		return true
	}
	return s.providerEnabled(providerName)
}

// refundRepo type-asserts s.repo to RefundRepository. Kept as a method
// (not a stored field set at construction) so NewService's signature never
// has to change -- every production Repository (*PGRepository) satisfies
// both interfaces; a test double that only implements Repository and never
// calls a refund method simply never exercises this path.
func (s *Service) refundRepo() (RefundRepository, error) {
	rr, ok := s.repo.(RefundRepository)
	if !ok {
		return nil, apperror.New(apperror.CodeInternal, "payment: repository does not support refunds")
	}
	return rr, nil
}

// CreatePayment creates a payment via the "charge" (auth+capture in one
// call) flow, idempotent on (tenantID, idempotencyKey, fingerprint of
// rawBody):
//
//   - same key + same fingerprint, already completed -> replay the stored
//     response, no provider call.
//   - same key + same fingerprint, still in flight (concurrent request) ->
//     wait briefly for the winner, then replay; if it doesn't finish in
//     time, report "in progress" rather than doing the work twice.
//   - same key + different fingerprint -> apperror.CodeIdempotencyConflict.
//   - new key -> do the work.
//
// idempotencyKey is forwarded to provider.Charge on every attempt (plan
// requirement: the PJP must be able to dedup on its side too).
func (s *Service) CreatePayment(ctx context.Context, in CreatePaymentInput, idempotencyKey string, rawBody []byte) (CreatePaymentOutput, error) {
	if idempotencyKey == "" {
		return CreatePaymentOutput{}, apperror.New(apperror.CodeValidation, "Idempotency-Key header is required")
	}
	if in.TenantID == "" || in.Currency == "" || in.Provider == "" || in.Amount <= 0 {
		return CreatePaymentOutput{}, apperror.New(apperror.CodeValidation, "tenant_id, currency, provider and a positive amount are required")
	}
	if err := ValidateCurrency(in.Currency); err != nil {
		return CreatePaymentOutput{}, err
	}
	if in.CallbackURL != nil {
		if err := ValidateCallbackURL(ctx, *in.CallbackURL); err != nil {
			return CreatePaymentOutput{}, err
		}
	}

	fingerprint := fingerprintOf(rawBody)

	reserve, err := s.idem.Reserve(ctx, in.TenantID, idempotency.ScopeCharge, idempotencyKey, fingerprint)
	if err != nil {
		return CreatePaymentOutput{}, fmt.Errorf("create payment: reserve idempotency key: %w", err)
	}

	switch reserve.State {
	case idempotency.StateConflict:
		return CreatePaymentOutput{}, apperror.New(apperror.CodeIdempotencyConflict, "idempotency key reused with a different request body")

	case idempotency.StateCompleted:
		out, err := decodeStoredResponse(reserve.Record.ResponseBody)
		if err != nil {
			return CreatePaymentOutput{}, fmt.Errorf("create payment: decode stored response: %w", err)
		}
		out.Replayed = true
		return out, nil

	case idempotency.StateInProgress:
		if out, ok := s.pollForCompletion(ctx, in.TenantID, idempotency.ScopeCharge, idempotencyKey); ok {
			out.Replayed = true
			return out, nil
		}
		return CreatePaymentOutput{}, apperror.New(apperror.CodeIdempotencyInProgress, "a request with this idempotency key is still being processed")

	case idempotency.StateReserved:
		// We own this key: do the work below.
	}

	return s.doCreatePayment(ctx, in, idempotencyKey)
}

// doCreatePayment is the Fase 3 async create-payment path (addendum): it
// does exactly three things in ONE Postgres transaction — insert the
// payment row (status pending), insert an outbox row carrying a
// ChargeTaskInput payload, done — and then completes the idempotency
// record with that "pending" acknowledgment and returns. It never calls
// providers.Get or a provider method; that happens later, in
// Service.ProcessCharge, invoked by the worker off the outbox/queue.
func (s *Service) doCreatePayment(ctx context.Context, in CreatePaymentInput, idempotencyKey string) (CreatePaymentOutput, error) {
	p := &Payment{
		TenantID:      in.TenantID,
		Livemode:      in.Livemode,
		Currency:      in.Currency,
		Amount:        in.Amount,
		OperationType: OperationCharge,
		Provider:      in.Provider,
		PaymentMethod: in.PaymentMethod,
		CallbackURL:   in.CallbackURL,
		Envelope:      in.Envelope,
	}

	taskInput := ChargeTaskInput{
		IdempotencyKey: idempotencyKey,
		TenantID:       in.TenantID,
		Livemode:       in.Livemode,
		Currency:       in.Currency,
		Amount:         in.Amount,
		Provider:       in.Provider,
		PaymentMethod:  in.PaymentMethod,
		Metadata:       in.Metadata,
		CallbackURL:    in.CallbackURL,
		// Fase 5 item 5: snapshot ctx's current span (the HTTP request span,
		// if the caller is rest.handleCreatePayment using c.UserContext())
		// so ProcessCharge can continue the same trace -- see
		// injectTraceCarrier's doc in worker.go.
		TraceCarrier: injectTraceCarrier(ctx),
	}

	err := s.repo.CreateWithOutbox(ctx, p, func(ctx context.Context, tx pgx.Tx) error {
		// p.ID is only populated once CreateWithOutbox's own INSERT runs,
		// which happens before this callback — safe to read here.
		taskInput.PaymentID = p.ID
		payload, err := json.Marshal(taskInput)
		if err != nil {
			return fmt.Errorf("create payment: marshal charge task payload: %w", err)
		}
		_, err = outbox.Insert(ctx, tx, queue.TaskTypeCharge, payload)
		return err
	})
	if err != nil {
		// Reservation is left in "reserved" state on this path (nothing
		// committed, including the reservation's completion) — a retry
		// with the same key re-attempts the same insert. See phase report
		// for the documented limitation (no reservation reaper/TTL yet).
		return CreatePaymentOutput{}, fmt.Errorf("create payment: %w", err)
	}

	out := CreatePaymentOutput{PaymentID: p.ID, Status: StatusPending}
	if err := s.completeIdempotency(ctx, in.TenantID, idempotencyKey, 202, out); err != nil {
		return CreatePaymentOutput{}, err
	}

	return out, nil
}

// ApplyProviderEvent applies a parsed webhook event to whichever row it
// refers to -- a payment (matched by provider_ref), a refund (matched by
// provider_refund_ref), or a payout (matched by provider_ref), tried in that
// order via resolveStagedMatch (reconcile.go). A single inbound webhook's
// provider ref can only ever belong to one of the three, since each is a
// reference the provider minted independently for that specific operation.
//
// If none of the three match evt.ProviderRef, this returns
// apperror.CodeNotFound and changes nothing — it does NOT stage the event
// itself. As of Fase 3, staging on this exact outcome is the caller's job:
// the REST webhook handler (internal/transport/rest/webhook.go) calls this
// first, and only falls back to webhook.Store.Stage when it sees
// CodeNotFound, so the "never 404, always 200, stage for later" rule lives
// at the transport edge while this method stays a plain, transport-agnostic
// "apply or tell me why not". Every lookup below must therefore keep
// surfacing CodeNotFound (never wrap it into something else) on a genuine
// miss, or that no-404 staging path silently stops working.
//
// Dedup (same provider_event_id applied twice) and stale-event rejection
// (older timestamp than the last applied event) are both enforced inside
// ApplyTransition/ApplyRefundTransition/ApplyPayoutTransition, atomically,
// under the same row lock as the state transition itself.
func (s *Service) ApplyProviderEvent(ctx context.Context, providerName string, evt provider.WebhookEvent) error {
	match, err := s.resolveStagedMatch(ctx, providerName, evt.ProviderRef)
	if err != nil {
		return err
	}

	var target Status
	switch evt.Status {
	case provider.StatusSucceeded:
		target = StatusSucceeded
	case provider.StatusFailed:
		target = StatusFailed
	default:
		return apperror.New(apperror.CodeValidation, "payment: webhook event status must be succeeded or failed to drive a transition")
	}

	var eventID *string
	if evt.ProviderEventID != "" {
		eventID = &evt.ProviderEventID
	}
	const eventType = "webhook_"
	// A webhook callback is the PJP calling us, not the customer acting
	// directly against this template's own API -- "system" per
	// InitiatedBy's taxonomy, same as every other worker-driven transition
	// today, regardless of which of the three tables below actually applies.
	const initiatedBy = InitiatedBySystem

	switch match.kind {
	case stagedMatchRefund:
		rr, err := s.refundRepo()
		if err != nil {
			return err
		}
		return rr.ApplyRefundTransition(ctx, RefundTransitionParams{
			RefundID:        match.id,
			To:              target,
			EventTS:         evt.OccurredAt,
			EventType:       eventType + string(target),
			Provider:        providerName,
			ProviderEventID: eventID,
			RawPayload:      evt.RawPayload,
			InitiatedBy:     initiatedBy,
		})
	case stagedMatchPayout:
		pr, err := s.payoutRepo()
		if err != nil {
			return err
		}
		return pr.ApplyPayoutTransition(ctx, PayoutTransitionParams{
			PayoutID:        match.id,
			To:              target,
			EventTS:         evt.OccurredAt,
			EventType:       eventType + string(target),
			Provider:        providerName,
			ProviderEventID: eventID,
			RawPayload:      evt.RawPayload,
			InitiatedBy:     initiatedBy,
		})
	default:
		return s.repo.ApplyTransition(ctx, TransitionParams{
			PaymentID:       match.id,
			To:              target,
			EventTS:         evt.OccurredAt,
			EventType:       eventType + string(target),
			Provider:        providerName,
			ProviderEventID: eventID,
			RawPayload:      evt.RawPayload,
			InitiatedBy:     initiatedBy,
		})
	}
}

// GetPayment fetches a payment by id -- the read side of the async
// create-payment flow: since CreatePayment's response is always "pending",
// callers poll this (via GET /payments/{id}) to observe the eventual
// outcome ProcessCharge (or a later webhook) drives it to.
//
// tenantID is the AUTHENTICATED caller's tenant (resolved by the REST
// middleware/gRPC interceptor, never a URL/request value) -- this is the
// ONLY Get path a REST/gRPC handler may call (CRITICAL fix: a payment id
// alone is guessable/enumerable, and this used to be unscoped by tenant
// entirely). A payment belonging to a different tenant reports
// CodeNotFound, exactly like a nonexistent id -- see
// Repository.GetForTenant's doc.
func (s *Service) GetPayment(ctx context.Context, id uuid.UUID, tenantID string) (*Payment, error) {
	return s.repo.GetForTenant(ctx, id, tenantID)
}

func (s *Service) completeIdempotency(ctx context.Context, tenantID, key string, statusCode int, out CreatePaymentOutput) error {
	body, err := json.Marshal(out)
	if err != nil {
		return fmt.Errorf("create payment: marshal response for idempotency store: %w", err)
	}
	if err := s.idem.Complete(ctx, tenantID, idempotency.ScopeCharge, key, statusCode, body); err != nil {
		return fmt.Errorf("create payment: complete idempotency record: %w", err)
	}
	return nil
}

func (s *Service) pollForCompletion(ctx context.Context, tenantID, scope, key string) (CreatePaymentOutput, bool) {
	deadline := time.Now().Add(s.inProgressPollTimeout)
	ticker := time.NewTicker(s.inProgressPollInterval)
	defer ticker.Stop()

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return CreatePaymentOutput{}, false
		case <-ticker.C:
		}

		rec, err := s.idem.Get(ctx, tenantID, scope, key)
		if err != nil {
			continue
		}
		if rec.Status == "completed" {
			out, err := decodeStoredResponse(rec.ResponseBody)
			if err != nil {
				return CreatePaymentOutput{}, false
			}
			return out, true
		}
	}
	return CreatePaymentOutput{}, false
}

func decodeStoredResponse(body []byte) (CreatePaymentOutput, error) {
	var out CreatePaymentOutput
	if err := json.Unmarshal(body, &out); err != nil {
		return CreatePaymentOutput{}, err
	}
	return out, nil
}

func fingerprintOf(rawBody []byte) string {
	sum := sha256.Sum256(rawBody)
	return hex.EncodeToString(sum[:])
}
