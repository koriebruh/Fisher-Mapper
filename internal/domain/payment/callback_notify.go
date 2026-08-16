package payment

import "context"

// CallbackPayload is the JSON body POSTed to a caller's callback_url once a
// payment/payout reaches a terminal status (succeeded/failed).
type CallbackPayload struct {
	ID          string `json:"id"`
	Status      string `json:"status"`
	Amount      int64  `json:"amount"`
	Currency    string `json:"currency"`
	ProviderRef string `json:"provider_ref,omitempty"`
}

// CallbackNotifier delivers payload to url. Injected (Service.
// WithCallbackNotifier) rather than this package importing net/http
// directly, mirroring providerEnabled/onReconciliationMismatch's plain-func
// pattern -- keeps the domain layer decoupled from the HTTP-client stack the
// same way it's decoupled from config/observability.
//
// Contract, per this task's explicit "Stub Cheap" scope (no retry/backoff/
// signing -- deferred, see the doc on notifyCallback's call sites): an
// implementation MUST be best-effort and MUST NOT block the caller
// indefinitely or panic. A delivery failure must never fail the task that
// triggered it or cause a queue retry -- notifyCallback below only logs,
// never returns an error to ProcessCharge/ProcessPayout.
type CallbackNotifier func(ctx context.Context, url string, payload CallbackPayload)

// notifyCallback fires s.callbackNotifier (if wired) for a payment/payout
// that just reached a terminal status, swallowing a nil notifier or a nil
// callbackURL as "nothing to do" -- callers never need their own nil-checks.
//
// Deliberately scoped to ProcessCharge/ProcessPayout only: webhook-driven
// (ApplyProviderEvent) and reconciliation-driven terminal transitions do
// NOT fire a callback. This is a decision, not an omission -- extending
// coverage to those paths is a reasonable next step, but out of scope for
// this task (charge/payout creation flows only).
func (s *Service) notifyCallback(ctx context.Context, callbackURL *string, id string, status Status, amount int64, currency, providerRef string) {
	if s.callbackNotifier == nil || callbackURL == nil {
		return
	}
	s.callbackNotifier(ctx, *callbackURL, CallbackPayload{
		ID:          id,
		Status:      string(status),
		Amount:      amount,
		Currency:    currency,
		ProviderRef: providerRef,
	})
}
