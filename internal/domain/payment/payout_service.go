package payment

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"

	"Fisher-Mapper/internal/domain/apperror"
	"Fisher-Mapper/internal/messaging/idempotency"
	"Fisher-Mapper/internal/messaging/outbox"
	"Fisher-Mapper/internal/platform/queue"
	"Fisher-Mapper/internal/provider"
)

// CreatePayoutInput is the domain-level input for Service.CreatePayout --
// the payout-flow analogue of CreatePaymentInput. Unlike CreateRefundInput,
// Provider IS a caller-supplied field: a payout has no parent charge row to
// derive it from.
type CreatePayoutInput struct {
	TenantID    string
	Livemode    bool
	Currency    string
	Amount      int64
	Provider    string
	Destination string
	Metadata    map[string]string

	// CallbackURL mirrors CreatePaymentInput.CallbackURL's doc exactly.
	CallbackURL *string

	// Envelope mirrors CreatePaymentInput.Envelope's doc exactly -- see there
	// for why RequestIP/RequestUserAgent/TraceID stay out of the gRPC
	// fingerprint basis. Payout must carry the SAME envelope completeness as
	// charge/refund, not a stripped-down version.
	Envelope
}

// CreatePayoutOutput mirrors CreateRefundOutput: async by construction (same
// outbox->queue->worker pattern as charge/refund), so a fresh call always
// reports StatusPending.
type CreatePayoutOutput struct {
	PayoutID uuid.UUID `json:"payout_id"`
	Status   Status    `json:"status"`

	Replayed bool `json:"-"`
}

// PayoutTaskInput is the outbox/queue payload for TaskTypePayout -- the
// payout-flow analogue of ChargeTaskInput/RefundTaskInput.
type PayoutTaskInput struct {
	PayoutID       uuid.UUID         `json:"payout_id"`
	IdempotencyKey string            `json:"idempotency_key"`
	TenantID       string            `json:"tenant_id"`
	Livemode       bool              `json:"livemode"`
	Currency       string            `json:"currency"`
	Amount         int64             `json:"amount"`
	Provider       string            `json:"provider"`
	Destination    string            `json:"destination"`
	Metadata       map[string]string `json:"metadata"`

	// CallbackURL mirrors ChargeTaskInput.CallbackURL's doc exactly.
	CallbackURL *string `json:"callback_url,omitempty"`

	// TraceCarrier mirrors ChargeTaskInput.TraceCarrier -- see worker.go's
	// injectTraceCarrier/extractTraceCarrierAndStartSpan doc.
	TraceCarrier map[string]string `json:"trace_carrier,omitempty"`
}

// CreatePayout creates a standalone payout (money OUT, independent of any
// prior charge), idempotent on (tenantID, ScopePayout, idempotencyKey,
// fingerprint) -- its own idempotency scope, distinct from charge/refund, so
// a tenant reusing the same key string across operation types never
// collides.
//
// Mirrors CreatePayment/CreateRefund's async shape exactly: reserve
// idempotency, insert the payout row (pending) + outbox row in one
// transaction, return immediately. The actual provider.Payout call happens
// later, in ProcessPayout, off the queue -- never inline here.
func (s *Service) CreatePayout(ctx context.Context, in CreatePayoutInput, idempotencyKey string, rawBody []byte) (CreatePayoutOutput, error) {
	if idempotencyKey == "" {
		return CreatePayoutOutput{}, apperror.New(apperror.CodeValidation, "Idempotency-Key header is required")
	}
	if in.TenantID == "" || in.Currency == "" || in.Provider == "" || in.Destination == "" || in.Amount <= 0 {
		return CreatePayoutOutput{}, apperror.New(apperror.CodeValidation, "tenant_id, currency, provider, destination and a positive amount are required")
	}
	if err := ValidateCurrency(in.Currency); err != nil {
		return CreatePayoutOutput{}, err
	}
	if in.CallbackURL != nil {
		if err := ValidateCallbackURL(ctx, *in.CallbackURL); err != nil {
			return CreatePayoutOutput{}, err
		}
	}

	fingerprint := fingerprintOf(rawBody)

	reserve, err := s.idem.Reserve(ctx, in.TenantID, idempotency.ScopePayout, idempotencyKey, fingerprint)
	if err != nil {
		return CreatePayoutOutput{}, fmt.Errorf("create payout: reserve idempotency key: %w", err)
	}

	switch reserve.State {
	case idempotency.StateConflict:
		return CreatePayoutOutput{}, apperror.New(apperror.CodeIdempotencyConflict, "idempotency key reused with a different request body")

	case idempotency.StateCompleted:
		out, err := decodeStoredPayoutResponse(reserve.Record.ResponseBody)
		if err != nil {
			return CreatePayoutOutput{}, fmt.Errorf("create payout: decode stored response: %w", err)
		}
		out.Replayed = true
		return out, nil

	case idempotency.StateInProgress:
		if out, ok := s.pollForPayoutCompletion(ctx, in.TenantID, idempotencyKey); ok {
			out.Replayed = true
			return out, nil
		}
		return CreatePayoutOutput{}, apperror.New(apperror.CodeIdempotencyInProgress, "a request with this idempotency key is still being processed")

	case idempotency.StateReserved:
		// We own this key: do the work below.
	}

	return s.doCreatePayout(ctx, in, idempotencyKey)
}

func (s *Service) doCreatePayout(ctx context.Context, in CreatePayoutInput, idempotencyKey string) (CreatePayoutOutput, error) {
	pr, err := s.payoutRepo()
	if err != nil {
		return CreatePayoutOutput{}, err
	}

	p := &Payout{
		TenantID:    in.TenantID,
		Livemode:    in.Livemode,
		Currency:    in.Currency,
		Amount:      in.Amount,
		Provider:    in.Provider,
		Destination: in.Destination,
		CallbackURL: in.CallbackURL,
		Envelope:    in.Envelope,
	}

	taskInput := PayoutTaskInput{
		IdempotencyKey: idempotencyKey,
		TenantID:       in.TenantID,
		Livemode:       in.Livemode,
		Currency:       in.Currency,
		Amount:         in.Amount,
		Provider:       in.Provider,
		Destination:    in.Destination,
		Metadata:       in.Metadata,
		CallbackURL:    in.CallbackURL,
		TraceCarrier:   injectTraceCarrier(ctx),
	}

	err = pr.CreatePayoutWithOutbox(ctx, p, func(ctx context.Context, tx pgx.Tx) error {
		// p.ID is only populated once CreatePayoutWithOutbox's own INSERT
		// runs, which happens before this callback.
		taskInput.PayoutID = p.ID
		payload, err := json.Marshal(taskInput)
		if err != nil {
			return fmt.Errorf("create payout: marshal payout task payload: %w", err)
		}
		_, err = outbox.Insert(ctx, tx, queue.TaskTypePayout, payload)
		return err
	})
	if err != nil {
		return CreatePayoutOutput{}, fmt.Errorf("create payout: %w", err)
	}

	out := CreatePayoutOutput{PayoutID: p.ID, Status: StatusPending}
	body, err := json.Marshal(out)
	if err != nil {
		return CreatePayoutOutput{}, fmt.Errorf("create payout: marshal response for idempotency store: %w", err)
	}
	if err := s.idem.Complete(ctx, in.TenantID, idempotency.ScopePayout, idempotencyKey, 202, body); err != nil {
		return CreatePayoutOutput{}, fmt.Errorf("create payout: complete idempotency record: %w", err)
	}

	return out, nil
}

// ProcessPayout is the worker-side handler for TaskTypePayout tasks -- the
// payout-flow mirror of ProcessCharge/ProcessRefund, sharing the identical
// invariant: a payout task may be delivered more than once, but the
// underlying provider.Payout call must happen AT MOST ONCE per payout. That
// guarantee comes from the same pending->processing CAS pattern, applied to
// the payouts table via ApplyPayoutTransition.
//
// Returning nil on a provider timeout/error is equally deliberate here: the
// payout stays "processing", resolved later by reconciliation (see
// ReconcilePayout in reconcile.go), never by an automatic retry -- a blind
// retry of a payout is a double disbursement, exactly the failure mode the
// "charge = no-auto-retry" invariant exists to prevent for charges.
func (s *Service) ProcessPayout(ctx context.Context, in PayoutTaskInput) error {
	ctx, span := extractTraceCarrierAndStartSpan(ctx, in.TraceCarrier, "ProcessPayout",
		attribute.String("payout_id", in.PayoutID.String()),
		attribute.String("provider", in.Provider),
	)
	defer span.End()

	if !s.isProviderEnabled(in.Provider) {
		return apperror.New(apperror.CodeProviderDisabled, "process payout: provider "+in.Provider+" is disabled via dynamic config")
	}

	prov, err := s.providers.Get(in.Provider)
	if err != nil {
		return err
	}

	pr, err := s.payoutRepo()
	if err != nil {
		return err
	}

	err = pr.ApplyPayoutTransition(ctx, PayoutTransitionParams{
		PayoutID:    in.PayoutID,
		To:          StatusProcessing,
		EventTS:     s.now(),
		EventType:   outboxEventProcessingStarted,
		Provider:    in.Provider,
		InitiatedBy: InitiatedBySystem,
	})
	if err != nil {
		switch apperror.CodeOf(err) {
		case apperror.CodeInvalidTransition, apperror.CodeTerminalState:
			// Redelivery of an already-handled payout task.
			return nil
		default:
			return fmt.Errorf("process payout: transition to processing: %w", err)
		}
	}

	if !s.isProviderEnabled(in.Provider) {
		slog.Error("[worker] ProcessPayout: provider disabled between CAS and provider call, payout stuck processing with no provider_ref",
			"component", "worker", "layer", "worker", "source", apperror.SourceInternal, "provider", in.Provider, "payout_id", in.PayoutID)
		return nil
	}

	breaker := s.breakerFor(in.Provider)
	if breaker != nil && !breaker.Allow() {
		slog.Warn("[worker] ProcessPayout: circuit breaker open, skipping provider call",
			"component", "worker", "layer", "worker", "source", apperror.SourceInternal, "provider", in.Provider, "payout_id", in.PayoutID)
		return nil
	}

	if s.bulkheadLimiter != nil {
		if err := s.bulkheadLimiter.Acquire(ctx, in.Provider); err != nil {
			return fmt.Errorf("process payout: bulkhead acquire: %w", err)
		}
		defer s.bulkheadLimiter.Release(in.Provider)
	}

	payoutResp, payoutErr := prov.Payout(ctx, provider.PayoutRequest{
		IdempotencyKey: in.IdempotencyKey,
		TenantID:       in.TenantID,
		Livemode:       in.Livemode,
		Amount:         in.Amount,
		Currency:       in.Currency,
		Destination:    in.Destination,
		Metadata:       in.Metadata,
	})

	if payoutErr != nil {
		if breaker != nil {
			breaker.RecordFailure()
		}
		// payoutErr is whatever the Provider implementation returned raw
		// (never itself an *apperror.Error) -- wrap it as CodeProviderError
		// purely so LogAttr classifies it correctly as SourceProvider rather
		// than falling through CodeOf's CodeInternal default, which would
		// mislabel a PJP failure as a bug in this codebase.
		wrapped := apperror.Wrap(apperror.CodeProviderError, "process payout: provider call failed", payoutErr)
		slog.Warn("[worker] ProcessPayout: provider call failed, no auto-retry, staying processing",
			"component", "worker", "layer", "worker", "provider", in.Provider, "payout_id", in.PayoutID, "error", payoutErr, apperror.LogAttr(wrapped))
		// See method doc: never retried, payout stays "processing".
		return nil
	}
	if breaker != nil {
		breaker.RecordSuccess()
	}

	var target Status
	switch payoutResp.Status {
	case provider.StatusSucceeded:
		target = StatusSucceeded
	case provider.StatusFailed:
		target = StatusFailed
	default:
		// Processing/unknown -- stays "processing", resolved later by
		// reconciliation.
	}

	if target != "" {
		var providerRefPtr *string
		if payoutResp.ProviderRef != "" {
			providerRefPtr = &payoutResp.ProviderRef
		}
		if err := pr.ApplyPayoutTransition(ctx, PayoutTransitionParams{
			PayoutID:    in.PayoutID,
			To:          target,
			EventTS:     s.now(),
			EventType:   "payout_" + string(target),
			Provider:    in.Provider,
			ProviderRef: providerRefPtr,
			InitiatedBy: InitiatedBySystem,
		}); err != nil {
			return fmt.Errorf("process payout: apply result: %w", err)
		}

		// Fired synchronously, detached context -- mirrors ProcessCharge's
		// identical call/comment exactly.
		s.notifyCallback(context.Background(), in.CallbackURL, in.PayoutID.String(), target, in.Amount, in.Currency, payoutResp.ProviderRef)
	} else if payoutResp.ProviderRef != "" {
		if err := pr.SetPayoutProviderRef(ctx, in.PayoutID, payoutResp.ProviderRef); err != nil {
			return fmt.Errorf("process payout: set provider ref: %w", err)
		}
	}

	return nil
}

// GetPayout fetches a payout by id -- the read side of the async payout
// flow, mirroring GetPayment/GetRefund (including tenantID's meaning: the
// AUTHENTICATED caller's tenant, never a URL/request value).
func (s *Service) GetPayout(ctx context.Context, id uuid.UUID, tenantID string) (*Payout, error) {
	pr, err := s.payoutRepo()
	if err != nil {
		return nil, err
	}
	return pr.GetPayoutForTenant(ctx, id, tenantID)
}

// payoutRepo type-asserts s.repo to PayoutRepository -- mirrors refundRepo's
// doc/reasoning exactly.
func (s *Service) payoutRepo() (PayoutRepository, error) {
	pr, ok := s.repo.(PayoutRepository)
	if !ok {
		return nil, apperror.New(apperror.CodeInternal, "payment: repository does not support payouts")
	}
	return pr, nil
}

func (s *Service) pollForPayoutCompletion(ctx context.Context, tenantID, key string) (CreatePayoutOutput, bool) {
	deadline := time.Now().Add(s.inProgressPollTimeout)
	ticker := time.NewTicker(s.inProgressPollInterval)
	defer ticker.Stop()

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return CreatePayoutOutput{}, false
		case <-ticker.C:
		}

		rec, err := s.idem.Get(ctx, tenantID, idempotency.ScopePayout, key)
		if err != nil {
			continue
		}
		if rec.Status == idempotency.StatusCompleted {
			out, err := decodeStoredPayoutResponse(rec.ResponseBody)
			if err != nil {
				return CreatePayoutOutput{}, false
			}
			return out, true
		}
	}
	return CreatePayoutOutput{}, false
}

func decodeStoredPayoutResponse(body []byte) (CreatePayoutOutput, error) {
	var out CreatePayoutOutput
	if err := json.Unmarshal(body, &out); err != nil {
		return CreatePayoutOutput{}, err
	}
	return out, nil
}
