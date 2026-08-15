package payment

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"Fisher-Mapper/internal/messaging/webhook"
	"Fisher-Mapper/internal/provider"
	"Fisher-Mapper/internal/provider/mock"
)

// createProcessingPayout drives a fresh payout to StatusProcessing via
// CreatePayout + ProcessPayout, with the mock configured to report
// "processing" -- the precondition every payout-reconciliation test needs,
// mirroring createSucceededPayment's role for payment tests.
func createProcessingPayout(t *testing.T, svc *Service, amount int64) *Payout {
	t.Helper()
	tenantID := uuid.NewString()
	key := uuid.NewString()
	in := CreatePayoutInput{TenantID: tenantID, Currency: "USD", Amount: amount, Provider: "mock", Destination: "bank_acct_test", Envelope: testEnvelope}

	out, err := svc.CreatePayout(context.Background(), in, key, payoutBodyFor(tenantID, amount))
	if err != nil {
		t.Fatalf("CreatePayout: %v", err)
	}
	if err := svc.ProcessPayout(context.Background(), PayoutTaskInput{
		PayoutID: out.PayoutID, IdempotencyKey: key, TenantID: tenantID,
		Currency: "USD", Amount: amount, Provider: "mock", Destination: "bank_acct_test",
	}); err != nil {
		t.Fatalf("ProcessPayout: %v", err)
	}

	pr, err := svc.repo.(*PGRepository).GetPayout(context.Background(), out.PayoutID)
	if err != nil {
		t.Fatalf("GetPayout: %v", err)
	}
	if pr.Status != StatusProcessing {
		t.Fatalf("payout status = %s, want processing", pr.Status)
	}
	return pr
}

// TestService_ReconcilePayout_ResolvesViaGetStatus_AmountCurrencyMatch is
// the payout-flow analogue of
// TestService_ReconcilePayment_ResolvesViaGetStatus_AmountCurrencyMatch:
// money OUT must get the identical stuck-processing recovery money IN gets.
func TestService_ReconcilePayout_ResolvesViaGetStatus_AmountCurrencyMatch(t *testing.T) {
	pool := testPool(t)
	mockProv := mock.New(mock.Config{Name: "mock", Status: provider.StatusProcessing})
	svc := newTestService(pool, mockProv)

	p := createProcessingPayout(t, svc, 750)

	mockProv.SetStatus(provider.StatusSucceeded)

	if err := svc.ReconcilePayout(context.Background(), p); err != nil {
		t.Fatalf("ReconcilePayout: %v", err)
	}

	repo := NewPGRepository(pool)
	after, err := repo.GetPayout(context.Background(), p.ID)
	if err != nil {
		t.Fatalf("GetPayout: %v", err)
	}
	if after.Status != StatusSucceeded {
		t.Errorf("payout status after reconciliation = %s, want succeeded", after.Status)
	}
}

// TestService_ReconcilePayout_AmountMismatch_NeverAutoSucceeds is the
// negative-case analogue: GetStatus reporting a mismatched amount/currency
// must never be trusted, even for a payout -- staying "processing" forever
// is safer than moving money based on an unverified provider response.
func TestService_ReconcilePayout_AmountMismatch_NeverAutoSucceeds(t *testing.T) {
	pool := testPool(t)
	mockProv := mock.New(mock.Config{Name: "mock", Status: provider.StatusProcessing})
	svc := newTestService(pool, mockProv)

	p := createProcessingPayout(t, svc, 900)

	mockProv.SetStatus(provider.StatusSucceeded)
	mockProv.SetGetStatusOverrides(999999, "EUR")

	if err := svc.ReconcilePayout(context.Background(), p); err != nil {
		t.Fatalf("ReconcilePayout: %v", err)
	}

	repo := NewPGRepository(pool)
	after, err := repo.GetPayout(context.Background(), p.ID)
	if err != nil {
		t.Fatalf("GetPayout: %v", err)
	}
	if after.Status != StatusProcessing {
		t.Errorf("payout status after amount/currency MISMATCH reconciliation = %s, want still processing", after.Status)
	}
}

// TestService_ListStuckPayouts_OnlyReturnsPayoutsOlderThanThreshold mirrors
// TestService_ListStuckProcessing_OnlyReturnsPaymentsOlderThanThreshold.
func TestService_ListStuckPayouts_OnlyReturnsPayoutsOlderThanThreshold(t *testing.T) {
	pool := testPool(t)
	mockProv := mock.New(mock.Config{Name: "mock", Status: provider.StatusProcessing})
	svc := newTestService(pool, mockProv)

	p := createProcessingPayout(t, svc, 400)

	stuck, err := svc.ListStuckPayouts(context.Background(), time.Hour)
	if err != nil {
		t.Fatalf("ListStuckPayouts: %v", err)
	}
	for _, s := range stuck {
		if s.ID == p.ID {
			t.Fatalf("payout %s returned as stuck under a 1-hour threshold moments after entering processing", s.ID)
		}
	}

	stuck, err = svc.ListStuckPayouts(context.Background(), time.Nanosecond)
	if err != nil {
		t.Fatalf("ListStuckPayouts: %v", err)
	}
	var found bool
	for _, s := range stuck {
		if s.ID == p.ID {
			found = true
		}
	}
	if !found {
		t.Errorf("payout %s not returned as stuck under a ~zero threshold", p.ID)
	}
}

// TestService_ApplyProviderEvent_MatchesPayoutByProviderRef is the payout
// half of the HIGH finding's webhook-join fix: a payout's own
// async-completion webhook (keyed by payouts.provider_ref, which
// FindPayoutByProviderRef did not even exist to look up before this fix)
// must be joinable once the payout row exists.
func TestService_ApplyProviderEvent_MatchesPayoutByProviderRef(t *testing.T) {
	pool := testPool(t)
	mockProv := mock.New(mock.Config{Name: "mock", Status: provider.StatusProcessing})
	svc := newTestService(pool, mockProv)

	p := createProcessingPayout(t, svc, 350)
	if p.ProviderRef == nil || *p.ProviderRef == "" {
		t.Fatal("expected provider_ref to be set after ProcessPayout")
	}

	evt := provider.WebhookEvent{
		ProviderRef: *p.ProviderRef,
		Status:      provider.StatusSucceeded,
		OccurredAt:  time.Now().UTC(),
	}
	if err := svc.ApplyProviderEvent(context.Background(), "mock", evt); err != nil {
		t.Fatalf("ApplyProviderEvent: %v", err)
	}

	after, err := NewPGRepository(pool).GetPayout(context.Background(), p.ID)
	if err != nil {
		t.Fatalf("GetPayout: %v", err)
	}
	if after.Status != StatusSucceeded {
		t.Errorf("payout status after ApplyProviderEvent = %s, want succeeded", after.Status)
	}
}

// TestService_SweepStagedWebhooks_ResolvesPayoutStagedEvent is the sweep-side
// analogue for payouts: a webhook staged for a payout's provider_ref must
// eventually resolve via the periodic sweep.
func TestService_SweepStagedWebhooks_ResolvesPayoutStagedEvent(t *testing.T) {
	pool := testPool(t)
	stagingStore := webhook.NewStore(pool)
	mockProv := mock.New(mock.Config{Name: "mock", Status: provider.StatusProcessing})
	svc := newTestService(pool, mockProv).WithWebhookStaging(stagingStore)

	p := createProcessingPayout(t, svc, 275)
	if p.ProviderRef == nil || *p.ProviderRef == "" {
		t.Fatal("expected provider_ref to be set after ProcessPayout")
	}

	eventID := uuid.NewString()
	payload, _ := json.Marshal(map[string]any{
		"provider_event_id": eventID,
		"provider_ref":      *p.ProviderRef,
		"event_type":        "payout.succeeded",
		"status":            "succeeded",
		"occurred_at":       time.Now().UTC(),
	})
	if err := stagingStore.Stage(context.Background(), "mock", eventID, *p.ProviderRef, payload); err != nil {
		t.Fatalf("Stage: %v", err)
	}

	matched, err := svc.SweepStagedWebhooks(context.Background())
	if err != nil {
		t.Fatalf("SweepStagedWebhooks: %v", err)
	}
	if matched < 1 {
		t.Errorf("SweepStagedWebhooks matched %d pairs, want at least 1", matched)
	}

	after, err := NewPGRepository(pool).GetPayout(context.Background(), p.ID)
	if err != nil {
		t.Fatalf("GetPayout: %v", err)
	}
	if after.Status != StatusSucceeded {
		t.Errorf("payout status after sweep = %s, want succeeded", after.Status)
	}

	var payoutID *uuid.UUID
	if err := pool.QueryRow(context.Background(),
		`SELECT payout_id FROM incoming_webhook_events WHERE provider = 'mock' AND event_id = $1`, eventID,
	).Scan(&payoutID); err != nil {
		t.Fatalf("query staged row: %v", err)
	}
	if payoutID == nil || *payoutID != p.ID {
		t.Errorf("incoming_webhook_events.payout_id = %v, want %s", payoutID, p.ID)
	}
}
