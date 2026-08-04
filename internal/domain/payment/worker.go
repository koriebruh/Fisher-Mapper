package payment

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"Fisher-Mapper/internal/apperror"
	"Fisher-Mapper/internal/provider"
	"Fisher-Mapper/internal/webhook"
)

// ChargeTaskInput is the JSON payload carried by the outbox row / queue task
// Service.doCreatePayment enqueues and Service.ProcessCharge consumes. It is
// the full set of fields ProcessCharge needs to make the (exactly one)
// provider.Charge call this payment gets.
type ChargeTaskInput struct {
	PaymentID      uuid.UUID         `json:"payment_id"`
	IdempotencyKey string            `json:"idempotency_key"`
	TenantID       string            `json:"tenant_id"`
	Livemode       bool              `json:"livemode"`
	Currency       string            `json:"currency"`
	Amount         int64             `json:"amount"`
	Provider       string            `json:"provider"`
	PaymentMethod  string            `json:"payment_method"`
	Metadata       map[string]string `json:"metadata"`
}

// ProcessCharge is the Fase 3 worker-side handler for TaskTypeCharge tasks:
// it reuses the exact Repository/state-machine/idempotency machinery Fase 2
// built, just invoked from the async path instead of inline in the HTTP
// handler.
//
// THE invariant this method exists to enforce (Fase 3 "Catatan desain
// wajib"): a charge task may be delivered more than once — the outbox
// relay is allowed to retry dispatch, and even a correctly-functioning
// queue can redeliver — but the underlying provider.Charge call must happen
// AT MOST ONCE per payment. That guarantee does not come from asynq's
// dedup, or from the outbox's dispatched flag, or from MaxRetry(0); it
// comes from the pending->processing transition below being a
// compare-and-swap under Repository.ApplyTransition's SELECT ... FOR UPDATE
// row lock: only the delivery that finds the payment still "pending" wins
// the right to call the provider. Every other delivery (redelivered,
// duplicated, racing) finds the payment already "processing" or terminal,
// gets apperror.CodeInvalidTransition/CodeTerminalState from Transition,
// and returns immediately without touching the provider.
//
// Returning nil (not an error) on a provider timeout/error is equally
// deliberate: from the queue's perspective this attempt is "done" (one
// provider call was made, which is the whole point), so it must never be
// retried. The payment stays "processing"; resolving that is reconciliation
// (Fase 4), not a queue retry. Returning an error here is reserved for
// genuinely unexpected failures (unregistered provider, a DB write
// failing) that SHOULD land in terminal_failures for manual inspection.
func (s *Service) ProcessCharge(ctx context.Context, in ChargeTaskInput) error {
	// Resolved BEFORE the CAS deliberately: an unregistered/misconfigured
	// provider is a config error that will fail identically on any
	// retry/redelivery, so there is nothing to gain by burning this
	// payment's one CAS-guarded provider-call attempt on it. Failing here
	// leaves the payment "pending" (not "processing"), which is the
	// correct resting state for "never actually attempted" rather than
	// "attempted and unresolved".
	prov, err := s.providers.Get(in.Provider)
	if err != nil {
		// Unregistered/misconfigured provider: genuinely unexpected,
		// surfaced as an error so it reaches terminal_failures.
		return err
	}

	err = s.repo.ApplyTransition(ctx, TransitionParams{
		PaymentID: in.PaymentID,
		To:        StatusProcessing,
		EventTS:   s.now(),
		EventType: "processing_started",
		Provider:  in.Provider,
	})
	if err != nil {
		switch apperror.CodeOf(err) {
		case apperror.CodeInvalidTransition, apperror.CodeTerminalState:
			// Redelivery of an already-handled charge task. The provider
			// was already called (or is being called right now by the
			// delivery that won the CAS) — do not call it again.
			return nil
		default:
			return fmt.Errorf("process charge: transition to processing: %w", err)
		}
	}

	breaker := s.breakerFor(in.Provider)
	if breaker != nil && !breaker.Allow() {
		// Circuit open for this provider: skip the call rather than pile
		// onto a provider that's already failing. Same resting state as a
		// timeout -- "processing", resolved later by reconciliation.
		slog.Warn("process charge: circuit breaker open, skipping provider call", "provider", in.Provider, "payment_id", in.PaymentID)
		return nil
	}

	if s.bulkheadLimiter != nil {
		if err := s.bulkheadLimiter.Acquire(ctx, in.Provider); err != nil {
			return fmt.Errorf("process charge: bulkhead acquire: %w", err)
		}
		defer s.bulkheadLimiter.Release(in.Provider)
	}

	chargeResp, chargeErr := prov.Charge(ctx, provider.ChargeRequest{
		IdempotencyKey: in.IdempotencyKey,
		TenantID:       in.TenantID,
		Livemode:       in.Livemode,
		Amount:         in.Amount,
		Currency:       in.Currency,
		PaymentMethod:  in.PaymentMethod,
		Metadata:       in.Metadata,
	})

	if chargeErr != nil {
		if breaker != nil {
			breaker.RecordFailure()
		}
		// See method doc: never retried, payment stays "processing".
		return nil
	}
	if breaker != nil {
		breaker.RecordSuccess()
	}

	var target Status
	switch chargeResp.Status {
	case provider.StatusSucceeded:
		target = StatusSucceeded
	case provider.StatusFailed:
		target = StatusFailed
	default:
		// Processing / Unknown: stays "processing" -- resolved later by
		// reconciliation, not here.
	}

	if target != "" {
		var providerRefPtr *string
		if chargeResp.ProviderRef != "" {
			providerRefPtr = &chargeResp.ProviderRef
		}
		if err := s.repo.ApplyTransition(ctx, TransitionParams{
			PaymentID:   in.PaymentID,
			To:          target,
			EventTS:     s.now(),
			EventType:   "charge_" + string(target),
			Provider:    in.Provider,
			ProviderRef: providerRefPtr,
		}); err != nil {
			return fmt.Errorf("process charge: apply result: %w", err)
		}
	} else if chargeResp.ProviderRef != "" {
		if err := s.repo.SetProviderRef(ctx, in.PaymentID, chargeResp.ProviderRef); err != nil {
			return fmt.Errorf("process charge: set provider ref: %w", err)
		}
	}

	// Webhook staging join (plan item 9): now that this payment's
	// provider_ref is known, look for any staged event that arrived before
	// this payment row existed and apply it.
	if chargeResp.ProviderRef != "" && s.staging != nil {
		parse := func(ctx context.Context, payload []byte) (provider.WebhookEvent, error) {
			return prov.ParseWebhook(ctx, provider.ParseWebhookRequest{Body: payload})
		}
		if _, jerr := webhook.Join(ctx, s.staging, parse, s.ApplyProviderEvent, in.Provider, chargeResp.ProviderRef, in.PaymentID); jerr != nil {
			slog.Error("process charge: webhook join failed", "error", jerr, "payment_id", in.PaymentID)
		}
	}

	return nil
}

func (s *Service) breakerFor(providerName string) breakerLike {
	if s.breakers == nil {
		return nil
	}
	return s.breakers.Get(providerName)
}

// breakerLike is the subset of *circuitbreaker.Breaker ProcessCharge needs,
// declared locally so this file doesn't have to import circuitbreaker just
// for a type name already satisfied structurally.
type breakerLike interface {
	Allow() bool
	RecordSuccess()
	RecordFailure()
}
