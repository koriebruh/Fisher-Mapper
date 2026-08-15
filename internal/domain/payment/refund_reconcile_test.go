package payment

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"Fisher-Mapper/internal/domain/apperror"
	"Fisher-Mapper/internal/messaging/webhook"
	"Fisher-Mapper/internal/provider"
	"Fisher-Mapper/internal/provider/mock"
)

// mockRefundProviderRef replicates mock.Mock's unexported providerRef("rfnd",
// idempotencyKey) derivation exactly, so a test can compute the refund's
// eventual provider_refund_ref BEFORE calling CreateRefund/ProcessRefund --
// the only way to stage a webhook for a refund ref that genuinely doesn't
// exist yet, rather than staging after the row (and its ref) already exist.
func mockRefundProviderRef(idempotencyKey string) string {
	sum := sha256.Sum256([]byte(idempotencyKey))
	return "rfnd_" + hex.EncodeToString(sum[:])[:20]
}

// createProcessingRefund drives a fresh refund to StatusProcessing via
// CreateRefund + ProcessRefund, with the parent payment first driven to
// StatusSucceeded on the default (succeeded) mock, then the mock flipped to
// "processing" for the refund's own provider.Refund call -- the precondition
// every refund-reconciliation test needs, mirroring createProcessingPayout's
// role for payout tests.
func createProcessingRefund(t *testing.T, pool *pgxpool.Pool, svc *Service, mockProv *mock.Mock, amount int64) *Refund {
	t.Helper()
	paymentID := createSucceededPayment(t, svc, amount+500)
	mockProv.SetStatus(provider.StatusProcessing)

	key := uuid.NewString()
	out, err := svc.CreateRefund(context.Background(), CreateRefundInput{
		PaymentID: paymentID, TenantID: uuid.NewString(), Amount: amount, Envelope: testEnvelope,
	}, key, refundBodyFor(amount))
	if err != nil {
		t.Fatalf("CreateRefund: %v", err)
	}
	ref, err := NewPGRepository(pool).GetRefund(context.Background(), out.RefundID)
	if err != nil {
		t.Fatalf("GetRefund: %v", err)
	}
	if err := svc.ProcessRefund(context.Background(), RefundTaskInput{
		RefundID: ref.ID, PaymentID: paymentID, IdempotencyKey: key,
		Amount: amount, Currency: "USD", Provider: "mock", PaymentProviderRef: derefOrEmpty(ref.ProviderRef),
	}); err != nil {
		t.Fatalf("ProcessRefund: %v", err)
	}

	after, err := NewPGRepository(pool).GetRefund(context.Background(), ref.ID)
	if err != nil {
		t.Fatalf("GetRefund: %v", err)
	}
	if after.Status != StatusProcessing {
		t.Fatalf("refund status = %s, want processing", after.Status)
	}
	return after
}

// TestService_ReconcileRefund_ResolvesViaGetStatus_AmountCurrencyMatch is
// the refund-flow analogue of
// TestService_ReconcilePayment_ResolvesViaGetStatus_AmountCurrencyMatch /
// TestService_ReconcilePayout_ResolvesViaGetStatus_AmountCurrencyMatch:
// a refund whose provider call legitimately returned "processing" must
// still be resolvable once the provider actually finishes.
func TestService_ReconcileRefund_ResolvesViaGetStatus_AmountCurrencyMatch(t *testing.T) {
	pool := testPool(t)
	mockProv := mock.New(mock.Config{Name: "mock"})
	svc := newTestService(pool, mockProv)

	ref := createProcessingRefund(t, pool, svc, mockProv, 250)

	mockProv.SetStatus(provider.StatusSucceeded)

	if err := svc.ReconcileRefund(context.Background(), ref); err != nil {
		t.Fatalf("ReconcileRefund: %v", err)
	}

	repo := NewPGRepository(pool)
	after, err := repo.GetRefund(context.Background(), ref.ID)
	if err != nil {
		t.Fatalf("GetRefund: %v", err)
	}
	if after.Status != StatusSucceeded {
		t.Errorf("refund status after reconciliation = %s, want succeeded", after.Status)
	}
}

// TestService_ReconcileRefund_AmountMismatch_NeverAutoSucceeds is the
// negative-case analogue: GetStatus reporting a mismatched amount/currency
// must never be trusted for a refund either -- staying "processing" forever
// is safer than moving money based on an unverified provider response.
func TestService_ReconcileRefund_AmountMismatch_NeverAutoSucceeds(t *testing.T) {
	pool := testPool(t)
	mockProv := mock.New(mock.Config{Name: "mock"})
	svc := newTestService(pool, mockProv)

	ref := createProcessingRefund(t, pool, svc, mockProv, 300)

	mockProv.SetStatus(provider.StatusSucceeded)
	mockProv.SetGetStatusOverrides(999999, "EUR")

	if err := svc.ReconcileRefund(context.Background(), ref); err != nil {
		t.Fatalf("ReconcileRefund: %v", err)
	}

	repo := NewPGRepository(pool)
	after, err := repo.GetRefund(context.Background(), ref.ID)
	if err != nil {
		t.Fatalf("GetRefund: %v", err)
	}
	if after.Status != StatusProcessing {
		t.Errorf("refund status after amount/currency MISMATCH reconciliation = %s, want still processing", after.Status)
	}
}

// TestService_ListStuckRefunds_OnlyReturnsRefundsOlderThanThreshold mirrors
// TestService_ListStuckPayouts_OnlyReturnsPayoutsOlderThanThreshold.
func TestService_ListStuckRefunds_OnlyReturnsRefundsOlderThanThreshold(t *testing.T) {
	pool := testPool(t)
	mockProv := mock.New(mock.Config{Name: "mock"})
	svc := newTestService(pool, mockProv)

	ref := createProcessingRefund(t, pool, svc, mockProv, 150)

	stuck, err := svc.ListStuckRefunds(context.Background(), time.Hour)
	if err != nil {
		t.Fatalf("ListStuckRefunds: %v", err)
	}
	for _, s := range stuck {
		if s.ID == ref.ID {
			t.Fatalf("refund %s returned as stuck under a 1-hour threshold moments after entering processing", s.ID)
		}
	}

	stuck, err = svc.ListStuckRefunds(context.Background(), time.Nanosecond)
	if err != nil {
		t.Fatalf("ListStuckRefunds: %v", err)
	}
	var found bool
	for _, s := range stuck {
		if s.ID == ref.ID {
			found = true
		}
	}
	if !found {
		t.Errorf("refund %s not returned as stuck under a ~zero threshold", ref.ID)
	}
}

// TestService_ApplyProviderEvent_MatchesRefundByProviderRefundRef is the
// HIGH finding's webhook-join half: a refund's own async-completion webhook
// (keyed by provider_refund_ref, never a payment's provider_ref) must be
// joinable once the refund row exists -- previously ApplyProviderEvent only
// ever tried the payments table, so this ref could never match anything.
func TestService_ApplyProviderEvent_MatchesRefundByProviderRefundRef(t *testing.T) {
	pool := testPool(t)
	mockProv := mock.New(mock.Config{Name: "mock"})
	svc := newTestService(pool, mockProv)

	ref := createProcessingRefund(t, pool, svc, mockProv, 220)
	if ref.ProviderRefundRef == nil || *ref.ProviderRefundRef == "" {
		t.Fatal("expected provider_refund_ref to be set after ProcessRefund")
	}

	evt := provider.WebhookEvent{
		ProviderRef: *ref.ProviderRefundRef,
		Status:      provider.StatusSucceeded,
		OccurredAt:  time.Now().UTC(),
	}
	if err := svc.ApplyProviderEvent(context.Background(), "mock", evt); err != nil {
		t.Fatalf("ApplyProviderEvent: %v", err)
	}

	after, err := NewPGRepository(pool).GetRefund(context.Background(), ref.ID)
	if err != nil {
		t.Fatalf("GetRefund: %v", err)
	}
	if after.Status != StatusSucceeded {
		t.Errorf("refund status after ApplyProviderEvent = %s, want succeeded", after.Status)
	}
}

// TestService_ApplyProviderEvent_UnknownRefReturnsNotFound proves the
// payment->refund->payout fallback chain still reports CodeNotFound (never
// swallowing it into something else) once all three lookups miss --
// rest/webhook.go's no-404 staging path depends on exactly this code.
func TestService_ApplyProviderEvent_UnknownRefReturnsNotFound(t *testing.T) {
	pool := testPool(t)
	mockProv := mock.New(mock.Config{Name: "mock"})
	svc := newTestService(pool, mockProv)

	evt := provider.WebhookEvent{ProviderRef: uuid.NewString(), Status: provider.StatusSucceeded, OccurredAt: time.Now().UTC()}
	err := svc.ApplyProviderEvent(context.Background(), "mock", evt)
	if apperror.CodeOf(err) != apperror.CodeNotFound {
		t.Errorf("CodeOf(err) = %v, want CodeNotFound (payment/refund/payout lookup chain exhausted)", apperror.CodeOf(err))
	}
}

// TestService_SweepStagedWebhooks_ResolvesRefundStagedEvent is the sweep-side
// analogue of TestService_SweepStagedWebhooks_ResolvesEventsStagedAfterInitialJoin,
// applied to a refund instead of a payment -- a webhook staged for a
// refund's provider_refund_ref must eventually resolve via the periodic
// sweep, not sit in incoming_webhook_events forever.
func TestService_SweepStagedWebhooks_ResolvesRefundStagedEvent(t *testing.T) {
	pool := testPool(t)
	stagingStore := webhook.NewStore(pool)
	mockProv := mock.New(mock.Config{Name: "mock"})
	svc := newTestService(pool, mockProv).WithWebhookStaging(stagingStore)

	ref := createProcessingRefund(t, pool, svc, mockProv, 200)
	if ref.ProviderRefundRef == nil || *ref.ProviderRefundRef == "" {
		t.Fatal("expected provider_refund_ref to be set after ProcessRefund")
	}

	eventID := uuid.NewString()
	payload, _ := json.Marshal(map[string]any{
		"provider_event_id": eventID,
		"provider_ref":      *ref.ProviderRefundRef,
		"event_type":        "refund.succeeded",
		"status":            "succeeded",
		"occurred_at":       time.Now().UTC(),
	})
	if err := stagingStore.Stage(context.Background(), "mock", eventID, *ref.ProviderRefundRef, payload); err != nil {
		t.Fatalf("Stage: %v", err)
	}

	matched, err := svc.SweepStagedWebhooks(context.Background())
	if err != nil {
		t.Fatalf("SweepStagedWebhooks: %v", err)
	}
	if matched < 1 {
		t.Errorf("SweepStagedWebhooks matched %d pairs, want at least 1", matched)
	}

	after, err := NewPGRepository(pool).GetRefund(context.Background(), ref.ID)
	if err != nil {
		t.Fatalf("GetRefund: %v", err)
	}
	if after.Status != StatusSucceeded {
		t.Errorf("refund status after sweep = %s, want succeeded", after.Status)
	}

	var refundID *uuid.UUID
	if err := pool.QueryRow(context.Background(),
		`SELECT refund_id FROM incoming_webhook_events WHERE provider = 'mock' AND event_id = $1`, eventID,
	).Scan(&refundID); err != nil {
		t.Fatalf("query staged row: %v", err)
	}
	if refundID == nil || *refundID != ref.ID {
		t.Errorf("incoming_webhook_events.refund_id = %v, want %s", refundID, ref.ID)
	}
}

// TestService_SweepStagedWebhooks_JoinsRefundWebhookStagedBeforeRowExisted
// is the HIGH finding's exact scenario, not just the sweep mechanism in
// general: a webhook for a refund's provider_refund_ref arrives (and is
// staged, no-404) BEFORE the refund row itself exists, then must join once
// CreateRefund/ProcessRefund creates that row and assigns that exact ref --
// mock.Mock's providerRef derivation is deterministic on the idempotency
// key, so the ref is computed up front and the webhook staged first.
func TestService_SweepStagedWebhooks_JoinsRefundWebhookStagedBeforeRowExisted(t *testing.T) {
	pool := testPool(t)
	stagingStore := webhook.NewStore(pool)
	mockProv := mock.New(mock.Config{Name: "mock"})
	svc := newTestService(pool, mockProv).WithWebhookStaging(stagingStore)

	paymentID := createSucceededPayment(t, svc, 900)

	key := uuid.NewString()
	futureRef := mockRefundProviderRef(key)

	// occurred_at must be a timestamp the PROVIDER would plausibly attach to
	// this event -- i.e. later than the refund's own pending->processing
	// transition, which CreateRefund/ProcessRefund below haven't even run
	// yet. A same-instant (or earlier) occurred_at would make Transition
	// reject this as a stale event once Join replays it (evt.OccurredAt <
	// last_event_at), which is a correct rejection, but not what this test
	// is exercising -- so back-date the STAGING order but not the event's
	// own clock.
	occurredAt := time.Now().Add(time.Minute).UTC()
	eventID := uuid.NewString()
	payload, _ := json.Marshal(map[string]any{
		"provider_event_id": eventID,
		"provider_ref":      futureRef,
		"event_type":        "refund.succeeded",
		"status":            "succeeded",
		"occurred_at":       occurredAt,
	})
	if err := stagingStore.Stage(context.Background(), "mock", eventID, futureRef, payload); err != nil {
		t.Fatalf("Stage (before refund row exists): %v", err)
	}

	// The staged event has nothing to match yet -- applying it directly must
	// report CodeNotFound (the no-404 handler's exact contract), never a
	// spurious success or a different error.
	applyErr := svc.ApplyProviderEvent(context.Background(), "mock", provider.WebhookEvent{
		ProviderRef: futureRef, Status: provider.StatusSucceeded, OccurredAt: time.Now().UTC(),
	})
	if apperror.CodeOf(applyErr) != apperror.CodeNotFound {
		t.Fatalf("ApplyProviderEvent before refund exists: CodeOf = %v, want CodeNotFound", apperror.CodeOf(applyErr))
	}

	// Provider stays "processing" so the refund row lands with the SAME ref
	// the webhook was staged against, and CreateRefund/ProcessRefund use the
	// SAME idempotency key so mock.Mock derives futureRef again.
	mockProv.SetStatus(provider.StatusProcessing)
	out, err := svc.CreateRefund(context.Background(), CreateRefundInput{
		PaymentID: paymentID, TenantID: uuid.NewString(), Amount: 400, Envelope: testEnvelope,
	}, key, refundBodyFor(400))
	if err != nil {
		t.Fatalf("CreateRefund: %v", err)
	}
	ref, err := NewPGRepository(pool).GetRefund(context.Background(), out.RefundID)
	if err != nil {
		t.Fatalf("GetRefund: %v", err)
	}
	if err := svc.ProcessRefund(context.Background(), RefundTaskInput{
		RefundID: ref.ID, PaymentID: paymentID, IdempotencyKey: key,
		Amount: 400, Currency: "USD", Provider: "mock", PaymentProviderRef: derefOrEmpty(ref.ProviderRef),
	}); err != nil {
		t.Fatalf("ProcessRefund: %v", err)
	}

	after, err := NewPGRepository(pool).GetRefund(context.Background(), ref.ID)
	if err != nil {
		t.Fatalf("GetRefund: %v", err)
	}
	if after.ProviderRefundRef == nil || *after.ProviderRefundRef != futureRef {
		t.Fatalf("refund provider_refund_ref = %v, want %q (must match the ref the webhook was staged against)", after.ProviderRefundRef, futureRef)
	}

	matched, err := svc.SweepStagedWebhooks(context.Background())
	if err != nil {
		t.Fatalf("SweepStagedWebhooks: %v", err)
	}
	if matched < 1 {
		t.Fatalf("SweepStagedWebhooks matched %d pairs, want at least 1 (the pre-staged refund webhook)", matched)
	}

	final, err := NewPGRepository(pool).GetRefund(context.Background(), ref.ID)
	if err != nil {
		t.Fatalf("GetRefund: %v", err)
	}
	if final.Status != StatusSucceeded {
		t.Errorf("refund status after sweep = %s, want succeeded (webhook staged BEFORE the row existed must still join)", final.Status)
	}
}
