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

// CreateRefundInput is the domain-level input for Service.CreateRefund.
// Provider is deliberately NOT a field here -- CreateRefundWithOutbox
// derives it (and provider_ref) from the locked parent payment row, so a
// caller can never point a refund at a different provider than the charge
// it targets.
type CreateRefundInput struct {
	PaymentID uuid.UUID
	TenantID  string
	Livemode  bool
	Currency  string
	Amount    int64

	// Envelope mirrors CreatePaymentInput.Envelope's doc exactly -- see there
	// for why RequestIP/RequestUserAgent/TraceID stay out of the gRPC
	// fingerprint basis.
	Envelope
}

// CreateRefundOutput mirrors CreatePaymentOutput: async by construction
// (same outbox->queue->worker pattern as charge, per the task's explicit
// "wire it through the same async outbox pattern as charge" instruction),
// so a fresh call always reports StatusPending.
type CreateRefundOutput struct {
	RefundID  uuid.UUID `json:"refund_id"`
	PaymentID uuid.UUID `json:"payment_id"`
	Status    Status    `json:"status"`

	Replayed bool `json:"-"`
}

// RefundTaskInput is the outbox/queue payload for TaskTypeRefund -- the
// refund-flow analogue of ChargeTaskInput.
type RefundTaskInput struct {
	RefundID       uuid.UUID `json:"refund_id"`
	PaymentID      uuid.UUID `json:"payment_id"`
	IdempotencyKey string    `json:"idempotency_key"`
	TenantID       string    `json:"tenant_id"`
	Livemode       bool      `json:"livemode"`
	Currency       string    `json:"currency"`
	Amount         int64     `json:"amount"`
	Provider       string    `json:"provider"`
	// PaymentProviderRef is the ORIGINAL charge's provider_ref, captured at
	// refund-create time -- provider.RefundRequest needs it, and by the time
	// ProcessRefund runs the payment row itself is no longer looked up.
	PaymentProviderRef string `json:"payment_provider_ref"`

	// TraceCarrier mirrors ChargeTaskInput.TraceCarrier -- see worker.go's
	// injectTraceCarrier/extractTraceCarrierAndStartSpan doc.
	TraceCarrier map[string]string `json:"trace_carrier,omitempty"`
}

// CreateRefund creates a refund against an existing (succeeded) payment,
// idempotent on (tenantID, ScopeRefund, idempotencyKey, fingerprint) --
// its own idempotency scope, distinct from charge (plan Decide Now item
// 10), so a tenant reusing the same key string for a charge and a refund
// never collides.
//
// Per the task's explicit instruction, this mirrors CreatePayment's async
// shape exactly: reserve idempotency, insert the refund row (pending) +
// outbox row in one transaction, return immediately. The actual
// provider.Refund call happens later, in ProcessRefund, off the queue --
// never inline here.
func (s *Service) CreateRefund(ctx context.Context, in CreateRefundInput, idempotencyKey string, rawBody []byte) (CreateRefundOutput, error) {
	if idempotencyKey == "" {
		return CreateRefundOutput{}, apperror.New(apperror.CodeValidation, "Idempotency-Key header is required")
	}
	if in.PaymentID == uuid.Nil || in.TenantID == "" || in.Amount <= 0 {
		return CreateRefundOutput{}, apperror.New(apperror.CodeValidation, "payment_id, tenant_id and a positive amount are required")
	}
	// Currency is validated only when the caller actually supplied one --
	// CreateRefundWithOutbox derives it from the locked parent payment when
	// omitted (the common case: REST/gRPC callers usually don't repeat the
	// original payment's currency), so an empty string here is not garbage
	// input, it's "use the parent's".
	if in.Currency != "" {
		if err := ValidateCurrency(in.Currency); err != nil {
			return CreateRefundOutput{}, err
		}
	}

	fingerprint := fingerprintOf(rawBody)

	reserve, err := s.idem.Reserve(ctx, in.TenantID, idempotency.ScopeRefund, idempotencyKey, fingerprint)
	if err != nil {
		return CreateRefundOutput{}, fmt.Errorf("create refund: reserve idempotency key: %w", err)
	}

	switch reserve.State {
	case idempotency.StateConflict:
		return CreateRefundOutput{}, apperror.New(apperror.CodeIdempotencyConflict, "idempotency key reused with a different request body")

	case idempotency.StateCompleted:
		out, err := decodeStoredRefundResponse(reserve.Record.ResponseBody)
		if err != nil {
			return CreateRefundOutput{}, fmt.Errorf("create refund: decode stored response: %w", err)
		}
		out.Replayed = true
		return out, nil

	case idempotency.StateInProgress:
		if out, ok := s.pollForRefundCompletion(ctx, in.TenantID, idempotencyKey); ok {
			out.Replayed = true
			return out, nil
		}
		return CreateRefundOutput{}, apperror.New(apperror.CodeIdempotencyInProgress, "a request with this idempotency key is still being processed")

	case idempotency.StateReserved:
		// We own this key: do the work below.
	}

	return s.doCreateRefund(ctx, in, idempotencyKey)
}

func (s *Service) doCreateRefund(ctx context.Context, in CreateRefundInput, idempotencyKey string) (CreateRefundOutput, error) {
	rr, err := s.refundRepo()
	if err != nil {
		return CreateRefundOutput{}, err
	}

	ref := &Refund{
		PaymentID:     in.PaymentID,
		TenantID:      in.TenantID,
		Livemode:      in.Livemode,
		Currency:      in.Currency,
		Amount:        in.Amount,
		OperationType: OperationRefund,
		Envelope:      in.Envelope,
	}

	taskInput := RefundTaskInput{
		IdempotencyKey: idempotencyKey,
		PaymentID:      in.PaymentID,
		TenantID:       in.TenantID,
		Livemode:       in.Livemode,
		Amount:         in.Amount,
		TraceCarrier:   injectTraceCarrier(ctx),
	}

	err = rr.CreateRefundWithOutbox(ctx, ref, func(ctx context.Context, tx pgx.Tx) error {
		// ref.ID/Provider/ProviderRef/Currency are only populated once
		// CreateRefundWithOutbox's own lock+insert has run, which happens
		// before this callback.
		taskInput.RefundID = ref.ID
		taskInput.Provider = ref.Provider
		taskInput.Currency = ref.Currency
		if ref.ProviderRef != nil {
			taskInput.PaymentProviderRef = *ref.ProviderRef
		}
		payload, err := json.Marshal(taskInput)
		if err != nil {
			return fmt.Errorf("create refund: marshal refund task payload: %w", err)
		}
		_, err = outbox.Insert(ctx, tx, queue.TaskTypeRefund, payload)
		return err
	})
	if err != nil {
		return CreateRefundOutput{}, fmt.Errorf("create refund: %w", err)
	}

	out := CreateRefundOutput{RefundID: ref.ID, PaymentID: ref.PaymentID, Status: StatusPending}
	body, err := json.Marshal(out)
	if err != nil {
		return CreateRefundOutput{}, fmt.Errorf("create refund: marshal response for idempotency store: %w", err)
	}
	if err := s.idem.Complete(ctx, in.TenantID, idempotency.ScopeRefund, idempotencyKey, 202, body); err != nil {
		return CreateRefundOutput{}, fmt.Errorf("create refund: complete idempotency record: %w", err)
	}

	return out, nil
}

// ProcessRefund is the Fase 4 worker-side handler for TaskTypeRefund tasks
// -- the refund-flow mirror of ProcessCharge, sharing the identical
// invariant: a refund task may be delivered more than once, but the
// underlying provider.Refund call must happen AT MOST ONCE per refund. That
// guarantee comes from the same pending->processing CAS pattern, applied to
// the refunds table via ApplyRefundTransition instead of payments.
//
// Returning nil on a provider timeout/error is equally deliberate here: the
// refund stays "processing", resolved later by reconciliation (or a future
// GetStatus-based refund reconciliation, out of scope for this phase -- see
// phase report), never by an automatic retry.
func (s *Service) ProcessRefund(ctx context.Context, in RefundTaskInput) error {
	ctx, span := extractTraceCarrierAndStartSpan(ctx, in.TraceCarrier, "ProcessRefund",
		attribute.String("refund_id", in.RefundID.String()),
		attribute.String("provider", in.Provider),
	)
	defer span.End()

	if !s.isProviderEnabled(in.Provider) {
		return apperror.New(apperror.CodeProviderDisabled, "process refund: provider "+in.Provider+" is disabled via dynamic config")
	}

	prov, err := s.providers.Get(in.Provider)
	if err != nil {
		return err
	}

	rr, err := s.refundRepo()
	if err != nil {
		return err
	}

	err = rr.ApplyRefundTransition(ctx, RefundTransitionParams{
		RefundID:    in.RefundID,
		To:          StatusProcessing,
		EventTS:     s.now(),
		EventType:   "processing_started",
		Provider:    in.Provider,
		InitiatedBy: InitiatedBySystem,
	})
	if err != nil {
		switch apperror.CodeOf(err) {
		case apperror.CodeInvalidTransition, apperror.CodeTerminalState:
			// Redelivery of an already-handled refund task.
			return nil
		default:
			return fmt.Errorf("process refund: transition to processing: %w", err)
		}
	}

	if !s.isProviderEnabled(in.Provider) {
		slog.Error("process refund: provider disabled between CAS and provider call, refund stuck processing", "provider", in.Provider, "refund_id", in.RefundID)
		return nil
	}

	breaker := s.breakerFor(in.Provider)
	if breaker != nil && !breaker.Allow() {
		slog.Warn("process refund: circuit breaker open, skipping provider call", "provider", in.Provider, "refund_id", in.RefundID)
		return nil
	}

	if s.bulkheadLimiter != nil {
		if err := s.bulkheadLimiter.Acquire(ctx, in.Provider); err != nil {
			return fmt.Errorf("process refund: bulkhead acquire: %w", err)
		}
		defer s.bulkheadLimiter.Release(in.Provider)
	}

	refundResp, refundErr := prov.Refund(ctx, provider.RefundRequest{
		IdempotencyKey: in.IdempotencyKey,
		ProviderRef:    in.PaymentProviderRef,
		Amount:         in.Amount,
		Currency:       in.Currency,
	})

	if refundErr != nil {
		if breaker != nil {
			breaker.RecordFailure()
		}
		return nil
	}
	if breaker != nil {
		breaker.RecordSuccess()
	}

	var target Status
	switch refundResp.Status {
	case provider.StatusSucceeded:
		target = StatusSucceeded
	case provider.StatusFailed:
		target = StatusFailed
	default:
		// Processing/unknown -- stays "processing".
	}

	if target != "" {
		var refundRefPtr *string
		if refundResp.ProviderRefundRef != "" {
			refundRefPtr = &refundResp.ProviderRefundRef
		}
		if err := rr.ApplyRefundTransition(ctx, RefundTransitionParams{
			RefundID:          in.RefundID,
			To:                target,
			EventTS:           s.now(),
			EventType:         "refund_" + string(target),
			Provider:          in.Provider,
			ProviderRefundRef: refundRefPtr,
			InitiatedBy:       InitiatedBySystem,
		}); err != nil {
			return fmt.Errorf("process refund: apply result: %w", err)
		}
	}

	return nil
}

// GetRefund fetches a refund by id -- the read side of the async refund
// flow, mirroring GetPayment.
func (s *Service) GetRefund(ctx context.Context, id uuid.UUID) (*Refund, error) {
	rr, err := s.refundRepo()
	if err != nil {
		return nil, err
	}
	return rr.GetRefund(ctx, id)
}

func (s *Service) pollForRefundCompletion(ctx context.Context, tenantID, key string) (CreateRefundOutput, bool) {
	deadline := time.Now().Add(s.inProgressPollTimeout)
	ticker := time.NewTicker(s.inProgressPollInterval)
	defer ticker.Stop()

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return CreateRefundOutput{}, false
		case <-ticker.C:
		}

		rec, err := s.idem.Get(ctx, tenantID, idempotency.ScopeRefund, key)
		if err != nil {
			continue
		}
		if rec.Status == "completed" {
			out, err := decodeStoredRefundResponse(rec.ResponseBody)
			if err != nil {
				return CreateRefundOutput{}, false
			}
			return out, true
		}
	}
	return CreateRefundOutput{}, false
}

func decodeStoredRefundResponse(body []byte) (CreateRefundOutput, error) {
	var out CreateRefundOutput
	if err := json.Unmarshal(body, &out); err != nil {
		return CreateRefundOutput{}, err
	}
	return out, nil
}
