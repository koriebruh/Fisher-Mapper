// Package provider defines the final PJP (payment gateway provider)
// interface and the request/response types every provider implementation
// (mock now, real PJPs later) is built against.
//
// Per plan "Invarian Uang — Decide Now" item 6, this method set is locked
// in now: Authorize/Capture are split from Charge so a card auth-then-capture
// flow never needs a signature change later, and every method takes a
// single request struct / returns a single response struct rather than
// positional parameters — adding a field to a struct does not break the N
// providers that already implement the interface; adding a parameter does.
package provider

import (
	"context"
	"time"
)

// Status is the provider-reported outcome of an operation. It is a
// provider-facing concept, deliberately distinct from
// internal/domain/payment.Status: the service layer maps one to the other
// so this package never depends on the domain package (which, in turn,
// depends on this one) — that would be an import cycle.
type Status string

const (
	StatusSucceeded  Status = "succeeded"
	StatusFailed     Status = "failed"
	StatusProcessing Status = "processing"
	// StatusUnknown covers the "provider call timed out / connection
	// dropped, outcome unresolved" case. Per plan item 12 ("Charge =
	// no-auto-retry"), the caller must never treat this as "safe to
	// retry" — resolution is via GetStatus reconciliation (Phase 4), not a
	// blind retry.
	StatusUnknown Status = "unknown"
)

// AuthorizeRequest reserves funds without capturing them.
type AuthorizeRequest struct {
	IdempotencyKey string
	TenantID       string
	Livemode       bool
	Amount         int64
	Currency       string
	PaymentMethod  string
	Metadata       map[string]string
}

type AuthorizeResponse struct {
	ProviderRef string
	Status      Status
	RawResponse []byte
}

// CaptureRequest captures funds previously authorized via Authorize.
type CaptureRequest struct {
	IdempotencyKey string
	ProviderRef    string
	Amount         int64
	Currency       string
}

type CaptureResponse struct {
	ProviderRef string
	Status      Status
	RawResponse []byte
}

// ChargeRequest performs authorize+capture in a single call (the simple
// flow). IdempotencyKey MUST be forwarded on every attempt (including
// retries with the same logical request) so the provider can dedup on its
// side too — this is a plan requirement, not an optional nicety.
type ChargeRequest struct {
	IdempotencyKey string
	TenantID       string
	Livemode       bool
	Amount         int64
	Currency       string
	PaymentMethod  string
	Metadata       map[string]string
}

type ChargeResponse struct {
	ProviderRef string
	Status      Status
	RawResponse []byte
}

// GetStatusRequest polls a provider for the current state of a previously
// created operation. Used for reconciliation of processing/unknown
// payments (Phase 4) — the interface method exists now per the "final
// method set" decision even though the reconciliation caller arrives later.
type GetStatusRequest struct {
	ProviderRef string
}

// GetStatusResponse includes Amount/Currency (added in Fase 4) precisely so
// reconciliation can verify them against the payment it stored BEFORE
// trusting this response enough to mark anything succeeded (plan Decide Now
// item 11: "Verifikasi amount+currency yang dibalikin PJP cocok sama yang
// diminta, sebelum mark succeeded") -- Status alone is not enough to act on
// safely.
type GetStatusResponse struct {
	Status      Status
	Amount      int64
	Currency    string
	RawResponse []byte
}

// RefundRequest refunds a previously succeeded charge, in whole or part.
type RefundRequest struct {
	IdempotencyKey string
	ProviderRef    string
	Amount         int64
	Currency       string
}

type RefundResponse struct {
	ProviderRefundRef string
	Status            Status
	RawResponse       []byte
}

// ParseWebhookRequest is the raw inbound webhook payload the provider sent.
// Signature/timestamp verification is the Verifier's job (internal/provider/auth),
// not this method's — ParseWebhook only knows how to decode the provider's
// wire format into a WebhookEvent.
type ParseWebhookRequest struct {
	Headers map[string]string
	Body    []byte
}

// WebhookEvent is the provider-agnostic result of parsing a webhook body.
type WebhookEvent struct {
	ProviderEventID string
	ProviderRef     string
	EventType       string
	Status          Status
	OccurredAt      time.Time
	RawPayload      []byte
}

// Provider is the final PJP interface. Every method set member is locked
// in by the plan; extending behavior later must be done by adding fields to
// the request/response structs above, not by changing this method set.
type Provider interface {
	// Name identifies the provider (matches its registry key).
	Name() string

	Authorize(ctx context.Context, req AuthorizeRequest) (AuthorizeResponse, error)
	Capture(ctx context.Context, req CaptureRequest) (CaptureResponse, error)
	Charge(ctx context.Context, req ChargeRequest) (ChargeResponse, error)
	GetStatus(ctx context.Context, req GetStatusRequest) (GetStatusResponse, error)
	Refund(ctx context.Context, req RefundRequest) (RefundResponse, error)
	ParseWebhook(ctx context.Context, req ParseWebhookRequest) (WebhookEvent, error)
}
