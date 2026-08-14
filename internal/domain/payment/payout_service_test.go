package payment

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/google/uuid"

	"Fisher-Mapper/internal/domain/apperror"
	"Fisher-Mapper/internal/provider/mock"
)

// payoutBodyFor mirrors bodyFor/refundBodyFor for payout request bodies.
func payoutBodyFor(tenantID string, amount int64) []byte {
	return []byte(fmt.Sprintf(`{"tenant_id":%q,"currency":"USD","amount":%d,"provider":"mock","destination":"bank_acct_test"}`, tenantID, amount))
}

// TestService_CreatePayout_ReturnsPendingAndEnqueuesOutbox is the payout-flow
// analogue of TestService_CreatePayment_ReturnsPendingAndEnqueuesOutbox:
// CreatePayout must never call the provider itself, and must be idempotent
// in its OWN scope (ScopePayout), distinct from charge/refund.
func TestService_CreatePayout_ReturnsPendingAndEnqueuesOutbox(t *testing.T) {
	pool := testPool(t)
	mockProv := mock.New(mock.Config{Name: "mock"})
	svc := newTestService(pool, mockProv)

	tenantID := uuid.NewString()
	key := uuid.NewString()
	raw := payoutBodyFor(tenantID, 1000)
	in := CreatePayoutInput{TenantID: tenantID, Currency: "USD", Amount: 1000, Provider: "mock", Destination: "bank_acct_test"}

	out, err := svc.CreatePayout(context.Background(), in, key, raw)
	if err != nil {
		t.Fatalf("CreatePayout: %v", err)
	}
	if out.Replayed {
		t.Error("fresh call reported Replayed=true, want false")
	}
	if out.Status != StatusPending {
		t.Errorf("Status = %s, want pending (CreatePayout must not call the provider)", out.Status)
	}
	if got := mockProv.CallCounts().Payout; got != 0 {
		t.Errorf("provider Payout called %d times by CreatePayout, want 0", got)
	}

	var outboxCount int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM outbox WHERE task_type = 'payout' AND status = 'pending' AND payload->>'payout_id' = $1`,
		out.PayoutID.String(),
	).Scan(&outboxCount); err != nil {
		t.Fatalf("count outbox rows: %v", err)
	}
	if outboxCount != 1 {
		t.Errorf("pending outbox rows for this payout = %d, want 1", outboxCount)
	}
}

// TestService_CreatePayout_SameKeyAsChargeScope_DoesNotCollide proves
// ScopePayout is a distinct idempotency namespace: reusing the exact same
// Idempotency-Key string a charge already completed with must not replay
// the charge's response or conflict -- it must create a fresh payout.
func TestService_CreatePayout_SameKeyAsChargeScope_DoesNotCollide(t *testing.T) {
	pool := testPool(t)
	mockProv := mock.New(mock.Config{Name: "mock"})
	svc := newTestService(pool, mockProv)

	sharedKey := uuid.NewString()

	tenantID := uuid.NewString()
	chargeIn := CreatePaymentInput{TenantID: tenantID, Currency: "USD", Amount: 500, Provider: "mock"}
	if _, err := svc.CreatePayment(context.Background(), chargeIn, sharedKey, bodyFor(tenantID, 500)); err != nil {
		t.Fatalf("CreatePayment: %v", err)
	}

	payoutIn := CreatePayoutInput{TenantID: tenantID, Currency: "USD", Amount: 500, Provider: "mock", Destination: "bank_acct_test"}
	out, err := svc.CreatePayout(context.Background(), payoutIn, sharedKey, payoutBodyFor(tenantID, 500))
	if err != nil {
		t.Fatalf("CreatePayout with a key already used by a charge: %v, want it to succeed in its own scope", err)
	}
	if out.Replayed {
		t.Error("CreatePayout reported Replayed=true against a charge-scope key, want a fresh payout in ScopePayout")
	}
}

// TestService_ProcessPayout_ConcurrentRedelivery_PayoutCalledOnce mirrors
// TestService_ProcessCharge_ConcurrentRedelivery_ChargeCalledOnce: a payout
// task redelivered N times must result in exactly one provider.Payout call.
func TestService_ProcessPayout_ConcurrentRedelivery_PayoutCalledOnce(t *testing.T) {
	pool := testPool(t)
	mockProv := mock.New(mock.Config{Name: "mock"})
	svc := newTestService(pool, mockProv)

	tenantID := uuid.NewString()
	key := uuid.NewString()
	raw := payoutBodyFor(tenantID, 700)
	in := CreatePayoutInput{TenantID: tenantID, Currency: "USD", Amount: 700, Provider: "mock", Destination: "bank_acct_test"}

	out, err := svc.CreatePayout(context.Background(), in, key, raw)
	if err != nil {
		t.Fatalf("CreatePayout: %v", err)
	}

	task := PayoutTaskInput{
		PayoutID: out.PayoutID, IdempotencyKey: key, TenantID: tenantID,
		Currency: "USD", Amount: 700, Provider: "mock", Destination: "bank_acct_test",
	}

	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = svc.ProcessPayout(context.Background(), task)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("ProcessPayout redelivery %d returned error: %v (must return nil even for a losing CAS)", i, err)
		}
	}
	if got := mockProv.CallCounts().Payout; got != 1 {
		t.Fatalf("provider Payout called %d times across %d concurrent redeliveries, want exactly 1 (double disbursement)", got, n)
	}

	p, err := NewPGRepository(pool).GetPayout(context.Background(), out.PayoutID)
	if err != nil {
		t.Fatalf("GetPayout: %v", err)
	}
	if p.Status != StatusSucceeded {
		t.Errorf("payout status = %s, want succeeded", p.Status)
	}
}

// TestService_ProcessPayout_ProviderError_NoRetry_StaysProcessing is the
// payout-flow analogue of TestService_ProcessCharge_ProviderTimeout_NoRetry_StaysProcessing:
// a payout call that errors must leave the payout "processing" (never
// retried automatically) -- a blind retry here means a double
// disbursement, which is worse than a double charge in the sense that
// there's no card network to eventually reverse it.
func TestService_ProcessPayout_ProviderError_NoRetry_StaysProcessing(t *testing.T) {
	pool := testPool(t)
	mockProv := mock.New(mock.Config{Name: "mock", ForceError: mock.ErrForced})
	svc := newTestService(pool, mockProv)

	tenantID := uuid.NewString()
	key := uuid.NewString()
	raw := payoutBodyFor(tenantID, 900)
	in := CreatePayoutInput{TenantID: tenantID, Currency: "USD", Amount: 900, Provider: "mock", Destination: "bank_acct_test"}

	out, err := svc.CreatePayout(context.Background(), in, key, raw)
	if err != nil {
		t.Fatalf("CreatePayout: %v", err)
	}
	task := PayoutTaskInput{
		PayoutID: out.PayoutID, IdempotencyKey: key, TenantID: tenantID,
		Currency: "USD", Amount: 900, Provider: "mock", Destination: "bank_acct_test",
	}

	if err := svc.ProcessPayout(context.Background(), task); err != nil {
		t.Fatalf("first ProcessPayout (provider error) returned error, want nil: %v", err)
	}
	if got := mockProv.CallCounts().Payout; got != 1 {
		t.Fatalf("provider Payout called %d times after first attempt, want 1", got)
	}

	repo := NewPGRepository(pool)
	p, err := repo.GetPayout(context.Background(), out.PayoutID)
	if err != nil {
		t.Fatalf("GetPayout: %v", err)
	}
	if p.Status != StatusProcessing {
		t.Fatalf("payout status after provider error = %s, want processing (no auto-retry)", p.Status)
	}

	// Simulate a redelivery of the same task.
	if err := svc.ProcessPayout(context.Background(), task); err != nil {
		t.Fatalf("second ProcessPayout returned error, want nil: %v", err)
	}
	if got := mockProv.CallCounts().Payout; got != 1 {
		t.Fatalf("provider Payout called %d times after redelivery, want still 1 (no double disbursement)", got)
	}
}

// TestService_ProcessPayout_ProviderDisabled_RejectedBeforeCAS mirrors
// TestService_ProcessRefund_ProviderDisabled_RejectedBeforeCAS.
func TestService_ProcessPayout_ProviderDisabled_RejectedBeforeCAS(t *testing.T) {
	pool := testPool(t)
	mockProv := mock.New(mock.Config{Name: "mock"})
	svc := newTestService(pool, mockProv).WithProviderEnabledCheck(func(string) bool { return false })

	tenantID := uuid.NewString()
	key := uuid.NewString()
	raw := payoutBodyFor(tenantID, 200)
	in := CreatePayoutInput{TenantID: tenantID, Currency: "USD", Amount: 200, Provider: "mock", Destination: "bank_acct_test"}

	out, err := svc.CreatePayout(context.Background(), in, key, raw)
	if err != nil {
		t.Fatalf("CreatePayout: %v", err)
	}

	err = svc.ProcessPayout(context.Background(), PayoutTaskInput{
		PayoutID: out.PayoutID, IdempotencyKey: key, TenantID: tenantID,
		Currency: "USD", Amount: 200, Provider: "mock", Destination: "bank_acct_test",
	})
	if err == nil {
		t.Fatal("ProcessPayout with provider disabled = nil error, want CodeProviderDisabled")
	}
	if apperror.CodeOf(err) != apperror.CodeProviderDisabled {
		t.Errorf("CodeOf(err) = %v, want %v", apperror.CodeOf(err), apperror.CodeProviderDisabled)
	}
	if got := mockProv.CallCounts().Payout; got != 0 {
		t.Errorf("provider Payout called %d times, want 0", got)
	}

	after, err := NewPGRepository(pool).GetPayout(context.Background(), out.PayoutID)
	if err != nil {
		t.Fatalf("GetPayout: %v", err)
	}
	if after.Status != StatusPending {
		t.Errorf("payout status = %s, want still pending (CAS never ran)", after.Status)
	}
}
