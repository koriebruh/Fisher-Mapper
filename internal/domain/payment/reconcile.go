// Fase 4 reconciliation support. The actual polling loop/actor lives in
// internal/reconciliation (a thin oklog/run actor); every piece of business
// logic that touches the state machine or the "never trust GetStatus
// blindly" money invariant lives here, in the domain service, for the same
// reason ProcessCharge does: it needs Repository.ApplyTransition under the
// same row lock, and it is exactly the kind of logic the existing
// integration-test suite is built to exercise.
package payment

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"Fisher-Mapper/internal/domain/apperror"
	"Fisher-Mapper/internal/provider"
	"Fisher-Mapper/internal/webhook"
)

// ListStuckProcessing returns payments that have been in StatusProcessing
// for longer than threshold — the reconciliation job's "poll processing
// stuck past some threshold" step.
func (s *Service) ListStuckProcessing(ctx context.Context, threshold time.Duration) ([]*Payment, error) {
	cutoff := s.now().Add(-threshold)
	return s.repo.ListProcessingOlderThan(ctx, cutoff)
}

// FindPaymentByProviderRef looks up a payment by (provider, providerRef) —
// exposed on Service (rather than requiring callers to depend on Repository
// directly) so internal/reconciliation's webhook sweep can resolve a staged
// event's provider_ref to a payment id without importing anything beyond
// this service.
func (s *Service) FindPaymentByProviderRef(ctx context.Context, providerName, providerRef string) (*Payment, error) {
	return s.repo.FindByProviderRef(ctx, providerName, providerRef)
}

// ReconcilePayment resolves ONE stuck-processing payment:
//  1. Join staged webhook events for this payment's provider_ref (cheap,
//     and if a webhook already arrived and was simply never joined, this
//     resolves the payment without needing to trust GetStatus at all).
//  2. If still not terminal, and the payment actually has a provider_ref
//     (i.e. a provider call was genuinely attempted — see doc note below),
//     call provider.GetStatus and verify BOTH amount and currency against
//     the stored payment before applying any transition (plan Decide Now
//     item 11: "Verifikasi amount+currency ... sebelum mark succeeded" —
//     this is the money invariant this method exists to enforce). A
//     mismatch is logged and left "processing", NEVER auto-marked
//     succeeded or failed — succeeded would move money we can't verify,
//     failed is a terminal, immutable claim we're not sure is true either.
//
// A payment stuck "processing" with NO provider_ref (the provider was never
// actually called — e.g. the Fase 4 provider-enabled flag was disabled
// between the outbox dispatch and the worker's second check, or Fase 3's
// circuit breaker was open) has nothing for GetStatus to query: there is no
// reference to ask the provider about. This is a documented, known gap
// (consistent with the pre-existing circuit-breaker-open case, which Fase 3
// already left unresolved the same way) rather than new Fase-4 scope — see
// the phase report.
func (s *Service) ReconcilePayment(ctx context.Context, p *Payment) error {
	prov, err := s.providers.Get(p.Provider)
	if err != nil {
		return fmt.Errorf("reconcile payment %s: %w", p.ID, err)
	}

	if p.ProviderRef != nil && s.staging != nil {
		parse := func(ctx context.Context, payload []byte) (provider.WebhookEvent, error) {
			return prov.ParseWebhook(ctx, provider.ParseWebhookRequest{Body: payload})
		}
		if _, jerr := webhook.Join(ctx, s.staging, parse, s.ApplyProviderEvent, p.Provider, *p.ProviderRef, p.ID); jerr != nil {
			slog.Error("reconcile payment: webhook join failed", "error", jerr, "payment_id", p.ID)
		}
	}

	// Re-fetch: the Join above may have already resolved this payment.
	fresh, err := s.repo.Get(ctx, p.ID)
	if err != nil {
		return fmt.Errorf("reconcile payment %s: re-fetch after join: %w", p.ID, err)
	}
	if IsTerminal(fresh.Status) {
		return nil
	}
	if fresh.ProviderRef == nil {
		slog.Warn("reconcile payment: stuck processing with no provider_ref, cannot query GetStatus (known gap)", "payment_id", p.ID)
		return nil
	}

	statusResp, err := prov.GetStatus(ctx, provider.GetStatusRequest{ProviderRef: *fresh.ProviderRef})
	if err != nil {
		slog.Warn("reconcile payment: GetStatus failed, will retry next cycle", "error", err, "payment_id", p.ID)
		return nil
	}

	var target Status
	switch statusResp.Status {
	case provider.StatusSucceeded:
		target = StatusSucceeded
	case provider.StatusFailed:
		target = StatusFailed
	default:
		// Still processing/unknown from the provider's point of view too —
		// nothing to apply this cycle.
		return nil
	}

	if statusResp.Amount != fresh.Amount || statusResp.Currency != fresh.Currency {
		slog.Error("reconciliation mismatch: GetStatus amount/currency does not match stored payment, refusing to apply",
			"payment_id", p.ID,
			"stored_amount", fresh.Amount, "stored_currency", fresh.Currency,
			"provider_amount", statusResp.Amount, "provider_currency", statusResp.Currency)
		return nil
	}

	if err := s.repo.ApplyTransition(ctx, TransitionParams{
		PaymentID: fresh.ID,
		To:        target,
		EventTS:   s.now(),
		EventType: "reconciliation_" + string(target),
		Provider:  fresh.Provider,
	}); err != nil {
		switch apperror.CodeOf(err) {
		case apperror.CodeInvalidTransition, apperror.CodeTerminalState, apperror.CodeStaleEvent:
			// Resolved by something else (a webhook that raced in) between
			// the re-fetch above and this call — benign.
			return nil
		default:
			return fmt.Errorf("reconcile payment %s: apply transition: %w", fresh.ID, err)
		}
	}
	return nil
}

// SweepStagedWebhooks re-attempts webhook.Join for every (provider,
// provider_ref) pair still carrying unprocessed staged events — the "sweep
// webhook events staged with no provider_ref match yet" gap the Fase 3
// report explicitly left to Fase 4 (ProcessCharge's Join call only ever
// fires ONCE, at charge-processing time, for that payment's provider_ref;
// anything staged after that point is otherwise never revisited). Returns
// the number of pairs for which a matching payment now exists (regardless
// of how many of that pair's events actually resolved).
func (s *Service) SweepStagedWebhooks(ctx context.Context) (int, error) {
	if s.staging == nil {
		return 0, nil
	}

	pairs, err := s.staging.FindUnprocessedProviderRefs(ctx)
	if err != nil {
		return 0, fmt.Errorf("sweep staged webhooks: %w", err)
	}

	var matched int
	for _, pair := range pairs {
		p, err := s.repo.FindByProviderRef(ctx, pair.Provider, pair.ProviderRef)
		if err != nil {
			if apperror.CodeOf(err) == apperror.CodeNotFound {
				// Still no payment for this ref — leave staged, try again
				// next sweep.
				continue
			}
			slog.Error("sweep staged webhooks: find by provider ref", "error", err, "provider", pair.Provider, "provider_ref", pair.ProviderRef)
			continue
		}

		prov, err := s.providers.Get(pair.Provider)
		if err != nil {
			slog.Error("sweep staged webhooks: provider lookup", "error", err, "provider", pair.Provider)
			continue
		}
		parse := func(ctx context.Context, payload []byte) (provider.WebhookEvent, error) {
			return prov.ParseWebhook(ctx, provider.ParseWebhookRequest{Body: payload})
		}
		if _, jerr := webhook.Join(ctx, s.staging, parse, s.ApplyProviderEvent, pair.Provider, pair.ProviderRef, p.ID); jerr != nil {
			slog.Error("sweep staged webhooks: join failed", "error", jerr, "provider", pair.Provider, "provider_ref", pair.ProviderRef)
			continue
		}
		matched++
	}
	return matched, nil
}
