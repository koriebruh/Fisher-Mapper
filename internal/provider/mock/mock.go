// Package mock implements provider.Provider for local development and
// tests. It is deliberately configurable (forced errors, forced latency)
// so callers can simulate the failure modes real PJPs exhibit — timeouts,
// outright rejection — without touching the interface or the service layer
// that consumes it.
package mock

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"Fisher-Mapper/internal/provider"
)

// Config controls the mock's simulated behavior.
type Config struct {
	// Name is the registry key this instance answers to.
	Name string

	// Status is returned by Charge/Authorize/Capture/Refund when neither
	// ForceError nor ForceTimeout apply. Defaults to StatusSucceeded (see
	// New).
	Status provider.Status

	// Latency, if > 0, is how long every call sleeps before responding —
	// used to simulate a slow provider (Phase 3's "one provider lambat
	// gak starvin provider lain" scenario) or, combined with a caller-side
	// context deadline shorter than Latency, a timeout.
	Latency time.Duration

	// ForceError, if non-nil, is returned by every call in place of a
	// normal response — simulates outright provider rejection/5xx.
	ForceError error

	// GetStatusAmountOverride / GetStatusCurrencyOverride, if non-zero /
	// non-empty, force GetStatus to report these values instead of the
	// amount/currency actually recorded from the Charge call that produced
	// ProviderRef — used to simulate a PJP returning a mismatched
	// amount/currency, exercising the reconciliation "never trust GetStatus
	// blindly" rejection path (plan Decide Now item 11) without needing a
	// real misbehaving provider.
	GetStatusAmountOverride   int64
	GetStatusCurrencyOverride string
}

// chargeRecord is what Charge remembers about a providerRef, so GetStatus
// can report the SAME amount/currency a reconciliation caller must verify
// against — a mock that always echoed back whatever amount the caller
// asked about would never be able to exercise a real mismatch check.
type chargeRecord struct {
	amount   int64
	currency string
}

// Mock is a provider.Provider implementation backed by in-memory state.
// Safe for concurrent use.
type Mock struct {
	cfg Config

	mu           sync.Mutex
	chargeCalls  int
	authCalls    int
	captureCalls int
	statusCalls  int
	refundCalls  int
	webhookCalls int
	charges      map[string]chargeRecord
}

// New builds a Mock. If cfg.Status is empty, it defaults to
// provider.StatusSucceeded so a zero-value Config behaves like a
// well-functioning provider.
func New(cfg Config) *Mock {
	if cfg.Status == "" {
		cfg.Status = provider.StatusSucceeded
	}
	return &Mock{cfg: cfg, charges: make(map[string]chargeRecord)}
}

func (m *Mock) Name() string { return m.cfg.Name }

// SetStatus updates the status Charge/Authorize/Capture/Refund/GetStatus
// report when neither ForceError nor a GetStatus override applies. Lets a
// test flip the mock's simulated outcome mid-test -- e.g. reconciliation
// tests that need "the provider was still processing when ProcessCharge
// ran, but has since finished" -- without constructing a fresh Mock and
// losing its per-providerRef charge-amount memory (used by GetStatus).
func (m *Mock) SetStatus(status provider.Status) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cfg.Status = status
}

// SetGetStatusOverrides sets GetStatusAmountOverride/GetStatusCurrencyOverride
// (see Config's doc comment) -- lets a test simulate a PJP's GetStatus
// response mismatching the amount/currency actually charged, after
// construction.
func (m *Mock) SetGetStatusOverrides(amount int64, currency string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cfg.GetStatusAmountOverride = amount
	m.cfg.GetStatusCurrencyOverride = currency
}

// wait blocks for cfg.Latency, returning ctx.Err() if the context is
// cancelled/deadline-exceeded first — this is what makes ForceTimeout-style
// tests possible: set Latency longer than the caller's context timeout.
func (m *Mock) wait(ctx context.Context) error {
	if m.cfg.Latency <= 0 {
		return nil
	}
	timer := time.NewTimer(m.cfg.Latency)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// providerRef derives a deterministic reference from the idempotency key so
// repeated calls with the same key simulate the provider's own dedup
// (rather than minting a new reference every attempt, which would defeat
// the "forward idempotency key to provider" requirement).
func providerRef(prefix, idempotencyKey string) string {
	sum := sha256.Sum256([]byte(idempotencyKey))
	return prefix + "_" + hex.EncodeToString(sum[:])[:20]
}

func rawResponse(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return b
}

// ErrForced is wrapped into whatever error value Config.ForceError set,
// exposed so tests can assert with errors.Is if they don't want to compare
// exact error values.
var ErrForced = errors.New("mock provider: forced error")

func (m *Mock) Authorize(ctx context.Context, req provider.AuthorizeRequest) (provider.AuthorizeResponse, error) {
	m.mu.Lock()
	m.authCalls++
	m.mu.Unlock()

	if err := m.wait(ctx); err != nil {
		return provider.AuthorizeResponse{}, err
	}
	if m.cfg.ForceError != nil {
		return provider.AuthorizeResponse{}, m.cfg.ForceError
	}

	ref := providerRef("auth", req.IdempotencyKey)
	return provider.AuthorizeResponse{
		ProviderRef: ref,
		Status:      m.cfg.Status,
		RawResponse: rawResponse(map[string]any{"provider_ref": ref, "status": m.cfg.Status}),
	}, nil
}

func (m *Mock) Capture(ctx context.Context, req provider.CaptureRequest) (provider.CaptureResponse, error) {
	m.mu.Lock()
	m.captureCalls++
	m.mu.Unlock()

	if err := m.wait(ctx); err != nil {
		return provider.CaptureResponse{}, err
	}
	if m.cfg.ForceError != nil {
		return provider.CaptureResponse{}, m.cfg.ForceError
	}

	return provider.CaptureResponse{
		ProviderRef: req.ProviderRef,
		Status:      m.cfg.Status,
		RawResponse: rawResponse(map[string]any{"provider_ref": req.ProviderRef, "status": m.cfg.Status}),
	}, nil
}

func (m *Mock) Charge(ctx context.Context, req provider.ChargeRequest) (provider.ChargeResponse, error) {
	m.mu.Lock()
	m.chargeCalls++
	m.mu.Unlock()

	if err := m.wait(ctx); err != nil {
		return provider.ChargeResponse{}, err
	}
	if m.cfg.ForceError != nil {
		return provider.ChargeResponse{}, m.cfg.ForceError
	}

	ref := providerRef("chg", req.IdempotencyKey)
	m.mu.Lock()
	m.charges[ref] = chargeRecord{amount: req.Amount, currency: req.Currency}
	m.mu.Unlock()
	return provider.ChargeResponse{
		ProviderRef: ref,
		Status:      m.cfg.Status,
		RawResponse: rawResponse(map[string]any{"provider_ref": ref, "status": m.cfg.Status}),
	}, nil
}

func (m *Mock) GetStatus(ctx context.Context, req provider.GetStatusRequest) (provider.GetStatusResponse, error) {
	m.mu.Lock()
	m.statusCalls++
	rec := m.charges[req.ProviderRef]
	m.mu.Unlock()

	if err := m.wait(ctx); err != nil {
		return provider.GetStatusResponse{}, err
	}
	if m.cfg.ForceError != nil {
		return provider.GetStatusResponse{}, m.cfg.ForceError
	}

	amount := rec.amount
	currency := rec.currency
	if m.cfg.GetStatusAmountOverride != 0 {
		amount = m.cfg.GetStatusAmountOverride
	}
	if m.cfg.GetStatusCurrencyOverride != "" {
		currency = m.cfg.GetStatusCurrencyOverride
	}

	return provider.GetStatusResponse{
		Status:      m.cfg.Status,
		Amount:      amount,
		Currency:    currency,
		RawResponse: rawResponse(map[string]any{"provider_ref": req.ProviderRef, "status": m.cfg.Status}),
	}, nil
}

func (m *Mock) Refund(ctx context.Context, req provider.RefundRequest) (provider.RefundResponse, error) {
	m.mu.Lock()
	m.refundCalls++
	m.mu.Unlock()

	if err := m.wait(ctx); err != nil {
		return provider.RefundResponse{}, err
	}
	if m.cfg.ForceError != nil {
		return provider.RefundResponse{}, m.cfg.ForceError
	}

	ref := providerRef("rfnd", req.IdempotencyKey)
	return provider.RefundResponse{
		ProviderRefundRef: ref,
		Status:            m.cfg.Status,
		RawResponse:       rawResponse(map[string]any{"provider_refund_ref": ref, "status": m.cfg.Status}),
	}, nil
}

// mockWebhookPayload is the JSON shape ParseWebhook expects — a stand-in
// for whatever wire format a real PJP would use.
type mockWebhookPayload struct {
	ProviderEventID string    `json:"provider_event_id"`
	ProviderRef     string    `json:"provider_ref"`
	EventType       string    `json:"event_type"`
	Status          string    `json:"status"`
	OccurredAt      time.Time `json:"occurred_at"`
}

func (m *Mock) ParseWebhook(ctx context.Context, req provider.ParseWebhookRequest) (provider.WebhookEvent, error) {
	m.mu.Lock()
	m.webhookCalls++
	m.mu.Unlock()

	if err := m.wait(ctx); err != nil {
		return provider.WebhookEvent{}, err
	}
	if m.cfg.ForceError != nil {
		return provider.WebhookEvent{}, m.cfg.ForceError
	}

	var payload mockWebhookPayload
	if err := json.Unmarshal(req.Body, &payload); err != nil {
		return provider.WebhookEvent{}, err
	}

	return provider.WebhookEvent{
		ProviderEventID: payload.ProviderEventID,
		ProviderRef:     payload.ProviderRef,
		EventType:       payload.EventType,
		Status:          provider.Status(payload.Status),
		OccurredAt:      payload.OccurredAt,
		RawPayload:      req.Body,
	}, nil
}

// CallCounts snapshots invocation counters for test assertions (e.g.
// "Charge must have been called exactly once despite two concurrent
// requests with the same idempotency key").
type CallCounts struct {
	Authorize int
	Capture   int
	Charge    int
	GetStatus int
	Refund    int
	Webhook   int
}

func (m *Mock) CallCounts() CallCounts {
	m.mu.Lock()
	defer m.mu.Unlock()
	return CallCounts{
		Authorize: m.authCalls,
		Capture:   m.captureCalls,
		Charge:    m.chargeCalls,
		GetStatus: m.statusCalls,
		Refund:    m.refundCalls,
		Webhook:   m.webhookCalls,
	}
}

var _ provider.Provider = (*Mock)(nil)
