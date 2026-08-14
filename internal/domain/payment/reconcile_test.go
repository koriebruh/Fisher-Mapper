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

// TestService_ReconcilePayment_ResolvesViaGetStatus_AmountCurrencyMatch is
// the Fase 4 mandatory reconciliation test: a payment stuck "processing"
// (simulating a crashed worker after the provider call but before the
// result was applied) gets resolved by ReconcilePayment calling GetStatus
// and, since amount+currency match, applying the transition.
func TestService_ReconcilePayment_ResolvesViaGetStatus_AmountCurrencyMatch(t *testing.T) {
	pool := testPool(t)
	repo := NewPGRepository(pool)
	// StatusProcessing: ProcessCharge will leave the payment "processing"
	// (not yet resolved) -- simulating a worker that made the provider call
	// but crashed before a webhook/GetStatus resolved it further, EXCEPT
	// here we simulate that the provider actually finished successfully in
	// the meantime (checked via GetStatus, not remembered from the Charge
	// call).
	mockProv := mock.New(mock.Config{Name: "mock", Status: provider.StatusProcessing})
	svc := newTestService(pool, mockProv)

	tenantID := uuid.NewString()
	key := uuid.NewString()
	raw := bodyFor(tenantID, 750)
	in := CreatePaymentInput{TenantID: tenantID, Currency: "USD", Amount: 750, Provider: "mock", Envelope: testEnvelope}
	out, err := svc.CreatePayment(context.Background(), in, key, raw)
	if err != nil {
		t.Fatalf("CreatePayment: %v", err)
	}
	if err := svc.ProcessCharge(context.Background(), chargeInputFor(out.PaymentID, key, in)); err != nil {
		t.Fatalf("ProcessCharge: %v", err)
	}

	p, err := repo.Get(context.Background(), out.PaymentID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if p.Status != StatusProcessing {
		t.Fatalf("payment status = %s, want processing", p.Status)
	}

	// Now the provider "finishes": flip the mock to report succeeded via
	// GetStatus (matching the recorded amount/currency from the Charge call
	// itself, per mock.Mock's chargeRecord tracking).
	mockProv.SetStatus(provider.StatusSucceeded)

	if err := svc.ReconcilePayment(context.Background(), p); err != nil {
		t.Fatalf("ReconcilePayment: %v", err)
	}

	after, err := repo.Get(context.Background(), out.PaymentID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if after.Status != StatusSucceeded {
		t.Errorf("payment status after reconciliation = %s, want succeeded", after.Status)
	}
}

// TestService_ReconcilePayment_AmountMismatch_NeverAutoSucceeds is the
// negative-case Fase 4 mandatory validation: GetStatus reporting a
// DIFFERENT amount than the stored payment must NOT be trusted -- the
// payment must stay "processing", never auto-marked succeeded (or failed).
func TestService_ReconcilePayment_AmountMismatch_NeverAutoSucceeds(t *testing.T) {
	pool := testPool(t)
	repo := NewPGRepository(pool)
	mockProv := mock.New(mock.Config{Name: "mock", Status: provider.StatusProcessing})
	svc := newTestService(pool, mockProv)

	tenantID := uuid.NewString()
	key := uuid.NewString()
	raw := bodyFor(tenantID, 900)
	in := CreatePaymentInput{TenantID: tenantID, Currency: "USD", Amount: 900, Provider: "mock", Envelope: testEnvelope}
	out, err := svc.CreatePayment(context.Background(), in, key, raw)
	if err != nil {
		t.Fatalf("CreatePayment: %v", err)
	}
	if err := svc.ProcessCharge(context.Background(), chargeInputFor(out.PaymentID, key, in)); err != nil {
		t.Fatalf("ProcessCharge: %v", err)
	}

	p, err := repo.Get(context.Background(), out.PaymentID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if p.Status != StatusProcessing {
		t.Fatalf("payment status = %s, want processing", p.Status)
	}

	// Provider now reports succeeded, but with a mismatched amount (and, to
	// be thorough, currency too) -- e.g. a corrupted/incorrect provider
	// response. ReconcilePayment must refuse to apply this.
	mockProv.SetStatus(provider.StatusSucceeded)
	mockProv.SetGetStatusOverrides(999999, "EUR")

	if err := svc.ReconcilePayment(context.Background(), p); err != nil {
		t.Fatalf("ReconcilePayment: %v", err)
	}

	after, err := repo.Get(context.Background(), out.PaymentID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if after.Status != StatusProcessing {
		t.Errorf("payment status after amount/currency MISMATCH reconciliation = %s, want still processing (must never auto-succeed on an unverified GetStatus)", after.Status)
	}
}

// TestService_ReconcilePayment_JoinsStagedWebhookBeforeTrustingGetStatus
// verifies ReconcilePayment resolves a payment via an already-staged
// webhook event WITHOUT needing GetStatus at all when one is available --
// exercising the "join staged webhook events for that provider_ref" half
// of the Fase 4 reconciliation task.
func TestService_ReconcilePayment_JoinsStagedWebhookBeforeTrustingGetStatus(t *testing.T) {
	pool := testPool(t)
	repo := NewPGRepository(pool)
	stagingStore := webhook.NewStore(pool)
	mockProv := mock.New(mock.Config{Name: "mock", Status: provider.StatusProcessing})
	svc := newTestService(pool, mockProv).WithWebhookStaging(stagingStore)

	tenantID := uuid.NewString()
	key := uuid.NewString()
	raw := bodyFor(tenantID, 650)
	in := CreatePaymentInput{TenantID: tenantID, Currency: "USD", Amount: 650, Provider: "mock", Envelope: testEnvelope}
	out, err := svc.CreatePayment(context.Background(), in, key, raw)
	if err != nil {
		t.Fatalf("CreatePayment: %v", err)
	}
	if err := svc.ProcessCharge(context.Background(), chargeInputFor(out.PaymentID, key, in)); err != nil {
		t.Fatalf("ProcessCharge: %v", err)
	}

	p, err := repo.Get(context.Background(), out.PaymentID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if p.Status != StatusProcessing || p.ProviderRef == nil {
		t.Fatalf("payment status = %s providerRef=%v, want processing with a provider_ref set", p.Status, p.ProviderRef)
	}

	// Stage a webhook event for this provider_ref AFTER ProcessCharge's own
	// (one-shot) Join call already ran -- simulating the Fase 3 known gap
	// this reconciliation job resolves.
	eventID := uuid.NewString()
	payload, _ := json.Marshal(map[string]any{
		"provider_event_id": eventID,
		"provider_ref":      *p.ProviderRef,
		"event_type":        "charge.succeeded",
		"status":            "succeeded",
		"occurred_at":       time.Now().UTC(),
	})
	if err := stagingStore.Stage(context.Background(), "mock", eventID, *p.ProviderRef, payload); err != nil {
		t.Fatalf("Stage: %v", err)
	}

	// Even with the provider itself still reporting "processing" via
	// GetStatus, the staged webhook (succeeded) must resolve the payment.
	if err := svc.ReconcilePayment(context.Background(), p); err != nil {
		t.Fatalf("ReconcilePayment: %v", err)
	}

	after, err := repo.Get(context.Background(), out.PaymentID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if after.Status != StatusSucceeded {
		t.Errorf("payment status after staged-webhook join = %s, want succeeded", after.Status)
	}
}

// TestService_ListStuckProcessing_OnlyReturnsPaymentsOlderThanThreshold
// exercises the repository query the reconciliation job's poll step relies
// on.
func TestService_ListStuckProcessing_OnlyReturnsPaymentsOlderThanThreshold(t *testing.T) {
	pool := testPool(t)
	mockProv := mock.New(mock.Config{Name: "mock", Status: provider.StatusProcessing})
	svc := newTestService(pool, mockProv)

	tenantID := uuid.NewString()
	key := uuid.NewString()
	raw := bodyFor(tenantID, 400)
	in := CreatePaymentInput{TenantID: tenantID, Currency: "USD", Amount: 400, Provider: "mock", Envelope: testEnvelope}
	out, err := svc.CreatePayment(context.Background(), in, key, raw)
	if err != nil {
		t.Fatalf("CreatePayment: %v", err)
	}
	if err := svc.ProcessCharge(context.Background(), chargeInputFor(out.PaymentID, key, in)); err != nil {
		t.Fatalf("ProcessCharge: %v", err)
	}

	// A huge threshold (1 hour) must NOT return a payment that just entered
	// "processing" moments ago.
	stuck, err := svc.ListStuckProcessing(context.Background(), time.Hour)
	if err != nil {
		t.Fatalf("ListStuckProcessing: %v", err)
	}
	for _, p := range stuck {
		if p.ID == out.PaymentID {
			t.Fatalf("payment %s returned as stuck under a 1-hour threshold moments after entering processing", p.ID)
		}
	}

	// A ~zero threshold must return it (membership check -- other
	// concurrently-running tests may also have processing payments, per the
	// package's documented shared-DB testing convention).
	stuck, err = svc.ListStuckProcessing(context.Background(), time.Nanosecond)
	if err != nil {
		t.Fatalf("ListStuckProcessing: %v", err)
	}
	var found bool
	for _, p := range stuck {
		if p.ID == out.PaymentID {
			found = true
		}
	}
	if !found {
		t.Errorf("payment %s not returned as stuck under a ~zero threshold", out.PaymentID)
	}
}

// TestService_SweepStagedWebhooks_ResolvesEventsStagedAfterInitialJoin is
// the Fase 4 "sweep webhook events staged with no provider_ref match yet"
// validation: a webhook staged for a provider_ref that already has a
// matching (terminal) payment, but arriving too late for ProcessCharge's
// one-shot Join call, must still eventually resolve via a sweep.
func TestService_SweepStagedWebhooks_ResolvesEventsStagedAfterInitialJoin(t *testing.T) {
	pool := testPool(t)
	stagingStore := webhook.NewStore(pool)
	mockProv := mock.New(mock.Config{Name: "mock", Status: provider.StatusProcessing})
	svc := newTestService(pool, mockProv).WithWebhookStaging(stagingStore)

	paymentID := createSucceededPayment(t, svc, 300) // note: Status:Processing config means this actually stays processing

	p, err := NewPGRepository(pool).Get(context.Background(), paymentID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if p.ProviderRef == nil {
		t.Fatal("expected provider_ref to be set after ProcessCharge")
	}

	eventID := uuid.NewString()
	payload, _ := json.Marshal(map[string]any{
		"provider_event_id": eventID,
		"provider_ref":      *p.ProviderRef,
		"event_type":        "charge.succeeded",
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

	after, err := NewPGRepository(pool).Get(context.Background(), paymentID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if after.Status != StatusSucceeded {
		t.Errorf("payment status after sweep = %s, want succeeded", after.Status)
	}
}
