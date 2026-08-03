package payment

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"Fisher-Mapper/internal/apperror"
	"Fisher-Mapper/internal/idempotency"
	"Fisher-Mapper/internal/provider"
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
}

// CreatePaymentOutput is what CreatePayment returns, and — serialized
// verbatim minus Replayed — what gets stored in the idempotency record for
// later replay.
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
// lives, called by thin REST (and later gRPC) handlers.
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
}

// NewService builds a Service with production defaults.
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

	fingerprint := fingerprintOf(rawBody)

	reserve, err := s.idem.Reserve(ctx, in.TenantID, idempotencyKey, fingerprint)
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
		if out, ok := s.pollForCompletion(ctx, in.TenantID, idempotencyKey); ok {
			out.Replayed = true
			return out, nil
		}
		return CreatePaymentOutput{}, apperror.New(apperror.CodeIdempotencyInProgress, "a request with this idempotency key is still being processed")

	case idempotency.StateReserved:
		// We own this key: do the work below.
	}

	return s.doCreatePayment(ctx, in, idempotencyKey)
}

func (s *Service) doCreatePayment(ctx context.Context, in CreatePaymentInput, idempotencyKey string) (CreatePaymentOutput, error) {
	p := &Payment{
		TenantID:      in.TenantID,
		Livemode:      in.Livemode,
		Currency:      in.Currency,
		Amount:        in.Amount,
		OperationType: OperationCharge,
		Provider:      in.Provider,
	}
	if err := s.repo.Create(ctx, p); err != nil {
		return CreatePaymentOutput{}, fmt.Errorf("create payment: %w", err)
	}

	// pending -> processing: mark the attempt as started before calling
	// the provider, so the state machine's recorded history always shows
	// a "processing" step was reached even when the provider responds
	// synchronously.
	if err := s.repo.ApplyTransition(ctx, TransitionParams{
		PaymentID: p.ID,
		To:        StatusProcessing,
		EventTS:   s.now(),
		EventType: "processing_started",
		Provider:  in.Provider,
	}); err != nil {
		return CreatePaymentOutput{}, fmt.Errorf("create payment: %w", err)
	}

	prov, err := s.providers.Get(in.Provider)
	if err != nil {
		// Reservation is left in "reserved" state on this path: a
		// misconfigured/unregistered provider is a caller/config error
		// that will fail identically on retry, so nothing is gained by
		// completing the idempotency record here. See phase report for
		// the documented limitation (no reservation reaper/TTL yet).
		return CreatePaymentOutput{}, err
	}

	chargeResp, chargeErr := prov.Charge(ctx, provider.ChargeRequest{
		IdempotencyKey: idempotencyKey,
		TenantID:       in.TenantID,
		Livemode:       in.Livemode,
		Amount:         in.Amount,
		Currency:       in.Currency,
		PaymentMethod:  in.PaymentMethod,
		Metadata:       in.Metadata,
	})

	if chargeErr != nil {
		// Plan item 12 ("Charge = no-auto-retry"): a provider error/timeout
		// on charge must NOT be retried automatically and must NOT be
		// treated as "failed" — it stays "processing" until reconciliation
		// (GetStatus, Phase 4) resolves it. We still complete the
		// idempotency record with this outcome so a retry with the same
		// key replays "processing" instead of calling the provider again.
		out := CreatePaymentOutput{PaymentID: p.ID, Status: StatusProcessing}
		if err := s.completeIdempotency(ctx, in.TenantID, idempotencyKey, 202, out); err != nil {
			return CreatePaymentOutput{}, err
		}
		return out, nil
	}

	out := CreatePaymentOutput{PaymentID: p.ID, ProviderRef: chargeResp.ProviderRef, Status: StatusProcessing}

	var target Status
	switch chargeResp.Status {
	case provider.StatusSucceeded:
		target = StatusSucceeded
	case provider.StatusFailed:
		target = StatusFailed
	default:
		// Processing / Unknown: stays "processing" — resolved later by
		// reconciliation, not here.
	}

	if target != "" {
		var providerRefPtr *string
		if chargeResp.ProviderRef != "" {
			providerRefPtr = &chargeResp.ProviderRef
		}
		if err := s.repo.ApplyTransition(ctx, TransitionParams{
			PaymentID:   p.ID,
			To:          target,
			EventTS:     s.now(),
			EventType:   "charge_" + string(target),
			Provider:    in.Provider,
			ProviderRef: providerRefPtr,
		}); err != nil {
			return CreatePaymentOutput{}, fmt.Errorf("create payment: %w", err)
		}
		out.Status = target
	} else if chargeResp.ProviderRef != "" {
		// Still processing, but we already have a provider ref worth
		// persisting for later GetStatus/webhook matching. A transition
		// with To == current status would be rejected by the state
		// machine (not in allowedTransitions), so this is a plain field
		// update instead of a state transition.
		if err := s.repo.SetProviderRef(ctx, p.ID, chargeResp.ProviderRef); err != nil {
			return CreatePaymentOutput{}, fmt.Errorf("create payment: %w", err)
		}
	}

	if err := s.completeIdempotency(ctx, in.TenantID, idempotencyKey, 201, out); err != nil {
		return CreatePaymentOutput{}, err
	}

	return out, nil
}

// ApplyProviderEvent applies a parsed webhook event to the payment it
// refers to (matched by provider_ref).
//
// Deviation from the full plan, scoped deliberately to stay inside Phase 2:
// the `incoming_webhook_events` staging table and its "never 404, join
// later" handling are explicitly Phase 3 work. If no payment exists yet for
// evt.ProviderRef, this returns apperror.CodeNotFound rather than staging
// the event — acceptable for Phase 2 because there is no HTTP webhook route
// wired to this method yet (validations 6/7 exercise it directly).
//
// Dedup (same provider_event_id applied twice) and stale-event rejection
// (older timestamp than the last applied event) are both enforced inside
// Repository.ApplyTransition, atomically, under the same row lock as the
// state transition itself.
func (s *Service) ApplyProviderEvent(ctx context.Context, providerName string, evt provider.WebhookEvent) error {
	p, err := s.repo.FindByProviderRef(ctx, providerName, evt.ProviderRef)
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

	return s.repo.ApplyTransition(ctx, TransitionParams{
		PaymentID:       p.ID,
		To:              target,
		EventTS:         evt.OccurredAt,
		EventType:       "webhook_" + string(target),
		Provider:        providerName,
		ProviderEventID: eventID,
		RawPayload:      evt.RawPayload,
	})
}

func (s *Service) completeIdempotency(ctx context.Context, tenantID, key string, statusCode int, out CreatePaymentOutput) error {
	body, err := json.Marshal(out)
	if err != nil {
		return fmt.Errorf("create payment: marshal response for idempotency store: %w", err)
	}
	if err := s.idem.Complete(ctx, tenantID, key, statusCode, body); err != nil {
		return fmt.Errorf("create payment: complete idempotency record: %w", err)
	}
	return nil
}

func (s *Service) pollForCompletion(ctx context.Context, tenantID, key string) (CreatePaymentOutput, bool) {
	deadline := time.Now().Add(s.inProgressPollTimeout)
	ticker := time.NewTicker(s.inProgressPollInterval)
	defer ticker.Stop()

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return CreatePaymentOutput{}, false
		case <-ticker.C:
		}

		rec, err := s.idem.Get(ctx, tenantID, key)
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
