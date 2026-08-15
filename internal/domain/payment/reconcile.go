// Fase 4 reconciliation support. The actual polling loop/actor lives in
// internal/messaging/reconciliation (a thin oklog/run actor); every piece of business
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

	"github.com/google/uuid"

	"Fisher-Mapper/internal/domain/apperror"
	"Fisher-Mapper/internal/messaging/webhook"
	"Fisher-Mapper/internal/provider"
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
// directly) so internal/messaging/reconciliation's webhook sweep can resolve a staged
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
		markProcessed := func(ctx context.Context, stagedID uuid.UUID) error {
			return s.staging.MarkProcessed(ctx, stagedID, p.ID)
		}
		if _, jerr := webhook.Join(ctx, s.staging, parse, s.ApplyProviderEvent, p.Provider, *p.ProviderRef, markProcessed); jerr != nil {
			slog.Error("reconcile payment: webhook join failed", "error", jerr, "payment_id", p.ID, apperror.LogAttr(jerr))
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
		slog.Warn("reconcile payment: stuck processing with no provider_ref, cannot query GetStatus (known gap)",
			"source", apperror.SourceInternal, "payment_id", p.ID)
		return nil
	}

	statusResp, err := prov.GetStatus(ctx, provider.GetStatusRequest{ProviderRef: *fresh.ProviderRef})
	if err != nil {
		// err is the raw error from the Provider's GetStatus (never itself an
		// *apperror.Error) -- wrap as CodeProviderError so LogAttr classifies
		// it as SourceProvider, not SourceInternal.
		wrapped := apperror.Wrap(apperror.CodeProviderError, "reconcile payment: GetStatus failed", err)
		slog.Warn("reconcile payment: GetStatus failed, will retry next cycle", "error", err, "payment_id", p.ID, apperror.LogAttr(wrapped))
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
			"source", apperror.SourceProvider, "payment_id", p.ID,
			"stored_amount", fresh.Amount, "stored_currency", fresh.Currency,
			"provider_amount", statusResp.Amount, "provider_currency", statusResp.Currency)
		if s.onReconciliationMismatch != nil {
			s.onReconciliationMismatch(ctx)
		}
		return nil
	}

	if err := s.repo.ApplyTransition(ctx, TransitionParams{
		PaymentID:   fresh.ID,
		To:          target,
		EventTS:     s.now(),
		EventType:   "reconciliation_" + string(target),
		Provider:    fresh.Provider,
		InitiatedBy: InitiatedBySystem,
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

// ListStuckPayouts is the payout analogue of ListStuckProcessing -- the
// reconciliation job's "poll processing stuck past some threshold" step,
// applied to payouts instead of payments. Payouts need the identical
// reconciliation sweep charges get: a stuck payout is exactly as dangerous
// left unresolved (money out, unknown outcome) as a stuck charge.
func (s *Service) ListStuckPayouts(ctx context.Context, threshold time.Duration) ([]*Payout, error) {
	pr, err := s.payoutRepo()
	if err != nil {
		return nil, err
	}
	cutoff := s.now().Add(-threshold)
	return pr.ListPayoutsProcessingOlderThan(ctx, cutoff)
}

// ReconcilePayout resolves ONE stuck-processing payout via provider.GetStatus,
// mirroring ReconcilePayment's shape exactly (webhook staging join is
// skipped -- payouts have no webhook-before-commit case documented in the
// plan, since nothing else references a payout by provider_ref before it
// exists): verify amount+currency against the stored payout before applying
// any transition (the same money invariant plan Decide Now item 11
// requires for charges), never trusting GetStatus blindly.
//
// A payout stuck "processing" with no provider_ref (never actually called --
// e.g. disabled between the outbox dispatch and the worker's second check,
// or the circuit breaker was open) has nothing for GetStatus to query,
// exactly like the equivalent payment case -- same documented, known gap.
func (s *Service) ReconcilePayout(ctx context.Context, p *Payout) error {
	pr, err := s.payoutRepo()
	if err != nil {
		return err
	}

	prov, err := s.providers.Get(p.Provider)
	if err != nil {
		return fmt.Errorf("reconcile payout %s: %w", p.ID, err)
	}

	fresh, err := pr.GetPayout(ctx, p.ID)
	if err != nil {
		return fmt.Errorf("reconcile payout %s: re-fetch: %w", p.ID, err)
	}
	if IsTerminal(fresh.Status) {
		return nil
	}
	if fresh.ProviderRef == nil {
		slog.Warn("reconcile payout: stuck processing with no provider_ref, cannot query GetStatus (known gap)",
			"layer", "reconciliation", "source", apperror.SourceInternal, "payout_id", p.ID)
		return nil
	}

	statusResp, err := prov.GetStatus(ctx, provider.GetStatusRequest{ProviderRef: *fresh.ProviderRef})
	if err != nil {
		// err is the raw error from the Provider's GetStatus (never itself an
		// *apperror.Error) -- wrap as CodeProviderError so LogAttr classifies
		// it as SourceProvider, not SourceInternal.
		wrapped := apperror.Wrap(apperror.CodeProviderError, "reconcile payout: GetStatus failed", err)
		slog.Warn("reconcile payout: GetStatus failed, will retry next cycle",
			"layer", "reconciliation", "error", err, "payout_id", p.ID, apperror.LogAttr(wrapped))
		return nil
	}

	var target Status
	switch statusResp.Status {
	case provider.StatusSucceeded:
		target = StatusSucceeded
	case provider.StatusFailed:
		target = StatusFailed
	default:
		return nil
	}

	if statusResp.Amount != fresh.Amount || statusResp.Currency != fresh.Currency {
		slog.Error("reconciliation mismatch: GetStatus amount/currency does not match stored payout, refusing to apply",
			"layer", "reconciliation", "source", apperror.SourceProvider, "payout_id", p.ID,
			"stored_amount", fresh.Amount, "stored_currency", fresh.Currency,
			"provider_amount", statusResp.Amount, "provider_currency", statusResp.Currency)
		if s.onReconciliationMismatch != nil {
			s.onReconciliationMismatch(ctx)
		}
		return nil
	}

	if err := pr.ApplyPayoutTransition(ctx, PayoutTransitionParams{
		PayoutID:    fresh.ID,
		To:          target,
		EventTS:     s.now(),
		EventType:   "reconciliation_" + string(target),
		Provider:    fresh.Provider,
		InitiatedBy: InitiatedBySystem,
	}); err != nil {
		switch apperror.CodeOf(err) {
		case apperror.CodeInvalidTransition, apperror.CodeTerminalState, apperror.CodeStaleEvent:
			return nil
		default:
			return fmt.Errorf("reconcile payout %s: apply transition: %w", fresh.ID, err)
		}
	}
	return nil
}

// ListStuckRefunds is the refund analogue of ListStuckProcessing/
// ListStuckPayouts -- a refund whose provider call legitimately returns
// "processing" (a common async-refund PSP behavior) needs the identical
// stuck-processing sweep charges and payouts get, not a permanent dead end.
func (s *Service) ListStuckRefunds(ctx context.Context, threshold time.Duration) ([]*Refund, error) {
	rr, err := s.refundRepo()
	if err != nil {
		return nil, err
	}
	cutoff := s.now().Add(-threshold)
	return rr.ListRefundsProcessingOlderThan(ctx, cutoff)
}

// ReconcileRefund resolves ONE stuck-processing refund via provider.GetStatus,
// mirroring ReconcilePayout's shape (webhook staging join is left to
// SweepStagedWebhooks, not repeated here -- see its doc for why the
// provider/refund lookup fallback lives there instead of a per-call Join):
// verify amount+currency against the stored refund before applying any
// transition (the same money invariant plan Decide Now item 11 requires for
// charges/payouts), never trusting GetStatus blindly.
//
// A refund stuck "processing" with no provider_refund_ref (never actually
// called, or the provider call errored/timed out before a reference came
// back) has nothing for GetStatus to query -- same documented, known gap as
// ReconcilePayment/ReconcilePayout.
func (s *Service) ReconcileRefund(ctx context.Context, ref *Refund) error {
	rr, err := s.refundRepo()
	if err != nil {
		return err
	}

	prov, err := s.providers.Get(ref.Provider)
	if err != nil {
		return fmt.Errorf("reconcile refund %s: %w", ref.ID, err)
	}

	fresh, err := rr.GetRefund(ctx, ref.ID)
	if err != nil {
		return fmt.Errorf("reconcile refund %s: re-fetch: %w", ref.ID, err)
	}
	if IsTerminal(fresh.Status) {
		return nil
	}
	if fresh.ProviderRefundRef == nil {
		slog.Warn("reconcile refund: stuck processing with no provider_refund_ref, cannot query GetStatus (known gap)",
			"source", apperror.SourceInternal, "refund_id", ref.ID)
		return nil
	}

	statusResp, err := prov.GetStatus(ctx, provider.GetStatusRequest{ProviderRef: *fresh.ProviderRefundRef})
	if err != nil {
		// err is the raw error from the Provider's GetStatus (never itself an
		// *apperror.Error) -- wrap as CodeProviderError so LogAttr classifies
		// it as SourceProvider, not SourceInternal.
		wrapped := apperror.Wrap(apperror.CodeProviderError, "reconcile refund: GetStatus failed", err)
		slog.Warn("reconcile refund: GetStatus failed, will retry next cycle", "error", err, "refund_id", ref.ID, apperror.LogAttr(wrapped))
		return nil
	}

	var target Status
	switch statusResp.Status {
	case provider.StatusSucceeded:
		target = StatusSucceeded
	case provider.StatusFailed:
		target = StatusFailed
	default:
		return nil
	}

	if statusResp.Amount != fresh.Amount || statusResp.Currency != fresh.Currency {
		slog.Error("reconciliation mismatch: GetStatus amount/currency does not match stored refund, refusing to apply",
			"source", apperror.SourceProvider, "refund_id", ref.ID,
			"stored_amount", fresh.Amount, "stored_currency", fresh.Currency,
			"provider_amount", statusResp.Amount, "provider_currency", statusResp.Currency)
		if s.onReconciliationMismatch != nil {
			s.onReconciliationMismatch(ctx)
		}
		return nil
	}

	if err := rr.ApplyRefundTransition(ctx, RefundTransitionParams{
		RefundID:    fresh.ID,
		To:          target,
		EventTS:     s.now(),
		EventType:   "reconciliation_" + string(target),
		Provider:    fresh.Provider,
		InitiatedBy: InitiatedBySystem,
	}); err != nil {
		switch apperror.CodeOf(err) {
		case apperror.CodeInvalidTransition, apperror.CodeTerminalState, apperror.CodeStaleEvent:
			return nil
		default:
			return fmt.Errorf("reconcile refund %s: apply transition: %w", fresh.ID, err)
		}
	}
	return nil
}

// stagedMatchKind distinguishes which table a resolveStagedMatch lookup
// resolved to -- ApplyProviderEvent/SweepStagedWebhooks both need this to
// pick the right ApplyTransition/MarkProcessed* variant, since a single
// inbound webhook's provider ref could belong to any of the three (a
// payment's own provider_ref, a refund's provider_refund_ref, or a payout's
// own provider_ref).
type stagedMatchKind int

const (
	stagedMatchPayment stagedMatchKind = iota
	stagedMatchRefund
	stagedMatchPayout
)

// stagedMatch is the result of resolveStagedMatch: which entity a
// (provider, providerRef) pair belongs to, and its id.
type stagedMatch struct {
	kind stagedMatchKind
	id   uuid.UUID
}

// markProcessed builds the webhook.MarkProcessedFunc closure matching this
// match's kind -- staging is passed in rather than read off Service so this
// stays usable from both ApplyProviderEvent (transport-agnostic, called
// directly by rest/webhook.go too) and SweepStagedWebhooks.
func (m stagedMatch) markProcessed(staging *webhook.Store) webhook.MarkProcessedFunc {
	switch m.kind {
	case stagedMatchRefund:
		return func(ctx context.Context, stagedID uuid.UUID) error {
			return staging.MarkProcessedRefund(ctx, stagedID, m.id)
		}
	case stagedMatchPayout:
		return func(ctx context.Context, stagedID uuid.UUID) error {
			return staging.MarkProcessedPayout(ctx, stagedID, m.id)
		}
	default:
		return func(ctx context.Context, stagedID uuid.UUID) error {
			return staging.MarkProcessed(ctx, stagedID, m.id)
		}
	}
}

// resolveStagedMatch tries payment, then refund (by provider_refund_ref --
// deliberately NOT refunds.provider_ref, which CreateRefundWithOutbox copies
// from the parent payment and would collide with the payment lookup above),
// then payout, in that order. A single (provider, providerRef) pair can only
// ever belong to one of the three: each is a reference the provider minted
// independently for that specific operation.
//
// This is the HIGH finding's core fix: ApplyProviderEvent/SweepStagedWebhooks
// previously only ever tried the payments table, so an inbound webhook
// carrying a refund's or payout's own async-completion event got staged by
// rest/webhook.go's no-404 handler and then could never be matched by
// anything -- it sat in incoming_webhook_events permanently.
func (s *Service) resolveStagedMatch(ctx context.Context, providerName, providerRef string) (*stagedMatch, error) {
	if p, err := s.repo.FindByProviderRef(ctx, providerName, providerRef); err == nil {
		return &stagedMatch{kind: stagedMatchPayment, id: p.ID}, nil
	} else if apperror.CodeOf(err) != apperror.CodeNotFound {
		return nil, err
	}

	if rr, rerr := s.refundRepo(); rerr == nil {
		if ref, err := rr.FindRefundByProviderRefundRef(ctx, providerName, providerRef); err == nil {
			return &stagedMatch{kind: stagedMatchRefund, id: ref.ID}, nil
		} else if apperror.CodeOf(err) != apperror.CodeNotFound {
			return nil, err
		}
	}

	if pr, perr := s.payoutRepo(); perr == nil {
		if po, err := pr.FindPayoutByProviderRef(ctx, providerName, providerRef); err == nil {
			return &stagedMatch{kind: stagedMatchPayout, id: po.ID}, nil
		} else if apperror.CodeOf(err) != apperror.CodeNotFound {
			return nil, err
		}
	}

	return nil, apperror.New(apperror.CodeNotFound, "payment: not found for provider ref")
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
		// Try payment, then refund (by provider_refund_ref), then payout, in
		// that order -- mirrors ApplyProviderEvent's own lookup chain. This
		// sweep needs to know WHICH row matched (to build the right
		// MarkProcessed* closure) before Join even runs, so it necessarily
		// re-does the lookup ApplyProviderEvent will do again internally once
		// it actually applies the transition -- the same duplication that
		// already existed here for the payment-only case.
		match, err := s.resolveStagedMatch(ctx, pair.Provider, pair.ProviderRef)
		if err != nil {
			if apperror.CodeOf(err) == apperror.CodeNotFound {
				// Still no payment/refund/payout for this ref — leave staged,
				// try again next sweep.
				continue
			}
			slog.Error("sweep staged webhooks: resolve match", "error", err, "provider", pair.Provider, "provider_ref", pair.ProviderRef, apperror.LogAttr(err))
			continue
		}

		prov, err := s.providers.Get(pair.Provider)
		if err != nil {
			slog.Error("sweep staged webhooks: provider lookup", "error", err, "provider", pair.Provider, apperror.LogAttr(err))
			continue
		}
		parse := func(ctx context.Context, payload []byte) (provider.WebhookEvent, error) {
			return prov.ParseWebhook(ctx, provider.ParseWebhookRequest{Body: payload})
		}
		if _, jerr := webhook.Join(ctx, s.staging, parse, s.ApplyProviderEvent, pair.Provider, pair.ProviderRef, match.markProcessed(s.staging)); jerr != nil {
			slog.Error("sweep staged webhooks: join failed", "error", jerr, "provider", pair.Provider, "provider_ref", pair.ProviderRef, apperror.LogAttr(jerr))
			continue
		}
		matched++
	}
	return matched, nil
}
