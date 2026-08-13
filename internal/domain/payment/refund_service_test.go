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

// refundBodyFor mirrors bodyFor (service_integration_test.go) for refund
// request bodies -- distinct fingerprint inputs per amount, same shape.
func refundBodyFor(amount int64) []byte {
	return []byte(fmt.Sprintf(`{"currency":"USD","amount":%d}`, amount))
}

// createSucceededPayment drives a fresh payment all the way to
// StatusSucceeded via the real CreatePayment + ProcessCharge path (the mock
// provider defaults to StatusSucceeded), returning the payment id -- the
// precondition every refund test needs (Decide Now item 10 constraints only
// apply to a succeeded charge).
func createSucceededPayment(t *testing.T, svc *Service, amount int64) uuid.UUID {
	t.Helper()
	tenantID := uuid.NewString()
	key := uuid.NewString()
	raw := bodyFor(tenantID, amount)
	in := CreatePaymentInput{TenantID: tenantID, Currency: "USD", Amount: amount, Provider: "mock"}

	out, err := svc.CreatePayment(context.Background(), in, key, raw)
	if err != nil {
		t.Fatalf("CreatePayment: %v", err)
	}
	if err := svc.ProcessCharge(context.Background(), chargeInputFor(out.PaymentID, key, in)); err != nil {
		t.Fatalf("ProcessCharge: %v", err)
	}
	return out.PaymentID
}

// TestService_CreateRefund_SucceedsAndEnqueuesOutbox is the refund-flow
// analogue of TestService_CreatePayment_ReturnsPendingAndEnqueuesOutbox:
// CreateRefund must never call the provider itself -- it only reserves
// idempotency (in the ScopeRefund namespace) and inserts refund(pending) +
// outbox rows in one transaction.
func TestService_CreateRefund_SucceedsAndEnqueuesOutbox(t *testing.T) {
	pool := testPool(t)
	mockProv := mock.New(mock.Config{Name: "mock"})
	svc := newTestService(pool, mockProv)

	paymentID := createSucceededPayment(t, svc, 1000)

	key := uuid.NewString()
	raw := refundBodyFor(400)
	out, err := svc.CreateRefund(context.Background(), CreateRefundInput{
		PaymentID: paymentID, TenantID: uuid.NewString(), Amount: 400,
	}, key, raw)
	if err != nil {
		t.Fatalf("CreateRefund: %v", err)
	}
	if out.Status != StatusPending {
		t.Errorf("Status = %s, want pending (CreateRefund must not call the provider)", out.Status)
	}
	if got := mockProv.CallCounts().Refund; got != 0 {
		t.Errorf("provider Refund called %d times by CreateRefund, want 0", got)
	}

	var outboxCount int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM outbox WHERE task_type = 'refund' AND status = 'pending' AND payload->>'refund_id' = $1`,
		out.RefundID.String(),
	).Scan(&outboxCount); err != nil {
		t.Fatalf("count outbox rows: %v", err)
	}
	if outboxCount != 1 {
		t.Errorf("pending outbox rows for this refund = %d, want 1", outboxCount)
	}
}

// TestService_CreateRefund_RejectsWhenPaymentNotSucceeded: a payment still
// "pending" (never processed) cannot be refunded.
func TestService_CreateRefund_RejectsWhenPaymentNotSucceeded(t *testing.T) {
	pool := testPool(t)
	mockProv := mock.New(mock.Config{Name: "mock"})
	svc := newTestService(pool, mockProv)

	tenantID := uuid.NewString()
	key := uuid.NewString()
	raw := bodyFor(tenantID, 500)
	in := CreatePaymentInput{TenantID: tenantID, Currency: "USD", Amount: 500, Provider: "mock"}
	out, err := svc.CreatePayment(context.Background(), in, key, raw)
	if err != nil {
		t.Fatalf("CreatePayment: %v", err)
	}
	// Deliberately do NOT call ProcessCharge -- payment stays "pending".

	_, err = svc.CreateRefund(context.Background(), CreateRefundInput{
		PaymentID: out.PaymentID, TenantID: uuid.NewString(), Amount: 100,
	}, uuid.NewString(), refundBodyFor(100))
	if err == nil {
		t.Fatal("CreateRefund against a non-succeeded payment = nil error, want rejection")
	}
	if apperror.CodeOf(err) != apperror.CodeInvalidTransition {
		t.Errorf("CodeOf(err) = %v, want %v", apperror.CodeOf(err), apperror.CodeInvalidTransition)
	}
}

// TestService_CreateRefund_SumExceedsOriginal_Rejected is the Fase 4
// mandatory validation: refunds summing to more than the original charge
// amount must be rejected by the constraint/service logic, not silently
// allowed. First refund (600) succeeds; a second attempt (500 more, total
// 1100 against a 1000 original) must be rejected — even though the FIRST
// refund is only "succeeded" (not yet further along), sum(pending +
// processing + succeeded) already accounts for it.
func TestService_CreateRefund_SumExceedsOriginal_Rejected(t *testing.T) {
	pool := testPool(t)
	mockProv := mock.New(mock.Config{Name: "mock"})
	svc := newTestService(pool, mockProv)

	paymentID := createSucceededPayment(t, svc, 1000)

	firstKey := uuid.NewString()
	firstOut, err := svc.CreateRefund(context.Background(), CreateRefundInput{
		PaymentID: paymentID, TenantID: uuid.NewString(), Amount: 600,
	}, firstKey, refundBodyFor(600))
	if err != nil {
		t.Fatalf("first CreateRefund (600): %v", err)
	}
	// Drive the first refund to succeeded via the worker path.
	first, err := NewPGRepository(pool).GetRefund(context.Background(), firstOut.RefundID)
	if err != nil {
		t.Fatalf("GetRefund: %v", err)
	}
	if err := svc.ProcessRefund(context.Background(), RefundTaskInput{
		RefundID: first.ID, PaymentID: paymentID, IdempotencyKey: firstKey,
		Amount: 600, Currency: "USD", Provider: "mock", PaymentProviderRef: derefOrEmpty(first.ProviderRef),
	}); err != nil {
		t.Fatalf("ProcessRefund (first): %v", err)
	}

	_, err = svc.CreateRefund(context.Background(), CreateRefundInput{
		PaymentID: paymentID, TenantID: uuid.NewString(), Amount: 500,
	}, uuid.NewString(), refundBodyFor(500))
	if err == nil {
		t.Fatal("second CreateRefund (500, total 1100 > original 1000) = nil error, want rejection")
	}
	if apperror.CodeOf(err) != apperror.CodeRefundLimitExceeded {
		t.Errorf("CodeOf(err) = %v, want %v", apperror.CodeOf(err), apperror.CodeRefundLimitExceeded)
	}

	// A refund that exactly fills the remaining 400 must still be allowed.
	okOut, err := svc.CreateRefund(context.Background(), CreateRefundInput{
		PaymentID: paymentID, TenantID: uuid.NewString(), Amount: 400,
	}, uuid.NewString(), refundBodyFor(400))
	if err != nil {
		t.Fatalf("CreateRefund for exactly the remaining amount (400): %v", err)
	}
	if okOut.Status != StatusPending {
		t.Errorf("Status = %s, want pending", okOut.Status)
	}
}

// TestService_CreateRefund_ConcurrentOverlappingRefunds_SumNeverExceeds is
// the concurrent-race version of the same invariant: N concurrent
// CreateRefund calls against the same payment, each individually under the
// limit, but collectively over it, must never ALL succeed -- the
// SELECT ... FOR UPDATE lock on the parent payment row serializes them, so
// the accepted subset's sum never exceeds the original amount.
func TestService_CreateRefund_ConcurrentOverlappingRefunds_SumNeverExceeds(t *testing.T) {
	pool := testPool(t)
	mockProv := mock.New(mock.Config{Name: "mock"})
	svc := newTestService(pool, mockProv)

	const originalAmount = 1000
	const perRefund = 300 // 4 x 300 = 1200 > 1000: at most 3 can be accepted
	paymentID := createSucceededPayment(t, svc, originalAmount)

	const n = 4
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = svc.CreateRefund(context.Background(), CreateRefundInput{
				PaymentID: paymentID, TenantID: uuid.NewString(), Amount: perRefund,
			}, uuid.NewString(), refundBodyFor(int64(1000+i))) // distinct fingerprints
		}(i)
	}
	wg.Wait()

	var accepted, rejected int
	for _, err := range errs {
		switch {
		case err == nil:
			accepted++
		case apperror.CodeOf(err) == apperror.CodeRefundLimitExceeded:
			rejected++
		default:
			t.Errorf("unexpected error: %v", err)
		}
	}
	if accepted+rejected != n {
		t.Fatalf("accepted(%d)+rejected(%d) != n(%d)", accepted, rejected, n)
	}
	if accepted*perRefund > originalAmount {
		t.Fatalf("accepted refunds sum to %d, exceeds original amount %d -- constraint violated", accepted*perRefund, originalAmount)
	}
	if accepted == 0 {
		t.Fatal("no refund was accepted at all, want at least floor(1000/300)=3")
	}

	var dbSum int64
	if err := pool.QueryRow(context.Background(),
		`SELECT coalesce(sum(amount), 0) FROM refunds WHERE payment_id = $1 AND status IN ('pending','processing','succeeded')`,
		paymentID,
	).Scan(&dbSum); err != nil {
		t.Fatalf("sum refunds: %v", err)
	}
	if dbSum > originalAmount {
		t.Fatalf("sum(refunds) in DB = %d, exceeds original amount %d", dbSum, originalAmount)
	}
}

// TestService_ProcessRefund_ConcurrentRedelivery_RefundCalledOnce mirrors
// TestService_ProcessCharge_ConcurrentRedelivery_ChargeCalledOnce: a refund
// task redelivered N times must result in exactly one provider.Refund call
// -- the same pending->processing CAS invariant, applied to refunds.
func TestService_ProcessRefund_ConcurrentRedelivery_RefundCalledOnce(t *testing.T) {
	pool := testPool(t)
	mockProv := mock.New(mock.Config{Name: "mock"})
	svc := newTestService(pool, mockProv)

	paymentID := createSucceededPayment(t, svc, 800)
	key := uuid.NewString()
	out, err := svc.CreateRefund(context.Background(), CreateRefundInput{
		PaymentID: paymentID, TenantID: uuid.NewString(), Amount: 300,
	}, key, refundBodyFor(300))
	if err != nil {
		t.Fatalf("CreateRefund: %v", err)
	}
	ref, err := NewPGRepository(pool).GetRefund(context.Background(), out.RefundID)
	if err != nil {
		t.Fatalf("GetRefund: %v", err)
	}
	task := RefundTaskInput{
		RefundID: ref.ID, PaymentID: paymentID, IdempotencyKey: key,
		Amount: 300, Currency: "USD", Provider: "mock", PaymentProviderRef: derefOrEmpty(ref.ProviderRef),
	}

	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = svc.ProcessRefund(context.Background(), task)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("ProcessRefund redelivery %d returned error: %v (must return nil even for a losing CAS)", i, err)
		}
	}
	if got := mockProv.CallCounts().Refund; got != 1 {
		t.Fatalf("provider Refund called %d times across %d concurrent redeliveries, want exactly 1 (double refund)", got, n)
	}
}

// TestService_ProcessRefund_ProviderDisabled_RejectedBeforeCAS is the Fase
// 4 provider-enabled check applied to refunds: disabled BEFORE any
// provider call, the refund must stay "pending" (never CAS'd to
// "processing"), and the provider must never be called.
func TestService_ProcessRefund_ProviderDisabled_RejectedBeforeCAS(t *testing.T) {
	pool := testPool(t)
	mockProv := mock.New(mock.Config{Name: "mock"})
	// Two Service instances sharing the same DB/provider: one with the
	// provider enabled (to get a payment into "succeeded" and create the
	// refund), one with it disabled (the thing actually under test) --
	// WithProviderEnabledCheck's effect must be scoped to ProcessRefund's
	// own check, not something CreateRefund/CreatePayment also trip over.
	setupSvc := newTestService(pool, mockProv)
	svc := newTestService(pool, mockProv).WithProviderEnabledCheck(func(string) bool { return false })

	paymentID := createSucceededPayment(t, setupSvc, 500)
	key := uuid.NewString()
	out, err := setupSvc.CreateRefund(context.Background(), CreateRefundInput{
		PaymentID: paymentID, TenantID: uuid.NewString(), Amount: 200,
	}, key, refundBodyFor(200))
	if err != nil {
		t.Fatalf("CreateRefund: %v", err)
	}
	ref, err := NewPGRepository(pool).GetRefund(context.Background(), out.RefundID)
	if err != nil {
		t.Fatalf("GetRefund: %v", err)
	}

	err = svc.ProcessRefund(context.Background(), RefundTaskInput{
		RefundID: ref.ID, PaymentID: paymentID, IdempotencyKey: key,
		Amount: 200, Currency: "USD", Provider: "mock", PaymentProviderRef: derefOrEmpty(ref.ProviderRef),
	})
	if err == nil {
		t.Fatal("ProcessRefund with provider disabled = nil error, want CodeProviderDisabled")
	}
	if apperror.CodeOf(err) != apperror.CodeProviderDisabled {
		t.Errorf("CodeOf(err) = %v, want %v", apperror.CodeOf(err), apperror.CodeProviderDisabled)
	}
	if got := mockProv.CallCounts().Refund; got != 0 {
		t.Errorf("provider Refund called %d times, want 0", got)
	}

	after, err := NewPGRepository(pool).GetRefund(context.Background(), ref.ID)
	if err != nil {
		t.Fatalf("GetRefund: %v", err)
	}
	if after.Status != StatusPending {
		t.Errorf("refund status = %s, want still pending (CAS never ran)", after.Status)
	}
}

func derefOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
