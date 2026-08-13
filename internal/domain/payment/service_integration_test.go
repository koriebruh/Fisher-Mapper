package payment

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"Fisher-Mapper/internal/domain/apperror"
	"Fisher-Mapper/internal/idempotency"
	"Fisher-Mapper/internal/provider"
	"Fisher-Mapper/internal/provider/mock"
	"Fisher-Mapper/internal/resilience/bulkhead"
)

// These tests exercise the real Postgres-backed Repository + idempotency
// Store together -- the atomic-insert race, replay/conflict, and the Fase 3
// pending->processing compare-and-swap under a real row lock can't be
// verified against a fake in-memory repository, since the whole point is
// the DB-level guarantee.
//
// Gated on TEST_POSTGRES_DSN so `go test ./...` stays green without a
// running Postgres; migrations 00001-00003 must already be applied (run
// `make migrate-up` against the same DSN, e.g. via docker-compose, before
// running these).
//
// Fase 3 addendum note: CreatePayment no longer calls the provider -- it
// only reserves idempotency and inserts payment(pending)+outbox rows. Every
// test below that needs a final outcome now does so explicitly by calling
// Service.ProcessCharge directly with the same ChargeTaskInput the worker
// would have received off the queue, exactly as the plan addendum sanctions
// ("gak perlu nunggu real async round-trip buat test deterministic").
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN not set; skipping DB-backed integration test")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect test postgres: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// singleProviderRegistry satisfies the service's providerRegistry
// interface with one fixed provider, regardless of the requested name --
// enough for these tests, which only ever use "mock".
type singleProviderRegistry struct {
	p provider.Provider
}

func (r singleProviderRegistry) Get(string) (provider.Provider, error) {
	return r.p, nil
}

func newTestService(pool *pgxpool.Pool, mockProv *mock.Mock) *Service {
	repo := NewPGRepository(pool)
	idemStore := idempotency.NewPGStore(pool)
	return NewService(repo, idemStore, singleProviderRegistry{mockProv})
}

// multiProviderRegistry dispatches Get by name to a fixed map -- used by
// the bulkhead test, which needs two distinct providers ("slow" and
// "fast") live at once.
type multiProviderRegistry struct {
	byName map[string]provider.Provider
}

func (r multiProviderRegistry) Get(name string) (provider.Provider, error) {
	p, ok := r.byName[name]
	if !ok {
		return nil, apperror.New(apperror.CodeProviderNotRegistered, "not registered: "+name)
	}
	return p, nil
}

func bodyFor(tenantID string, amount int64) []byte {
	return []byte(fmt.Sprintf(`{"tenant_id":%q,"currency":"USD","amount":%d,"provider":"mock"}`, tenantID, amount))
}

// chargeInputFor builds the ChargeTaskInput a worker would have received
// off the queue for a CreatePayment call with these exact arguments -- used
// by tests to invoke ProcessCharge directly instead of waiting on a real
// async round-trip.
func chargeInputFor(paymentID uuid.UUID, key string, in CreatePaymentInput) ChargeTaskInput {
	return ChargeTaskInput{
		PaymentID:      paymentID,
		IdempotencyKey: key,
		TenantID:       in.TenantID,
		Livemode:       in.Livemode,
		Currency:       in.Currency,
		Amount:         in.Amount,
		Provider:       in.Provider,
		PaymentMethod:  in.PaymentMethod,
		Metadata:       in.Metadata,
	}
}

// TestService_CreatePayment_ReturnsPendingAndEnqueuesOutbox is the Fase 3
// re-grounding of what CreatePayment itself guarantees: a fresh call
// reserves idempotency, inserts the payment (pending) and an outbox row in
// one transaction, and returns immediately -- no provider call at all.
func TestService_CreatePayment_ReturnsPendingAndEnqueuesOutbox(t *testing.T) {
	pool := testPool(t)
	mockProv := mock.New(mock.Config{Name: "mock"})
	svc := newTestService(pool, mockProv)

	tenantID := uuid.NewString()
	key := uuid.NewString()
	raw := bodyFor(tenantID, 1000)
	in := CreatePaymentInput{TenantID: tenantID, Currency: "USD", Amount: 1000, Provider: "mock"}

	out, err := svc.CreatePayment(context.Background(), in, key, raw)
	if err != nil {
		t.Fatalf("CreatePayment: %v", err)
	}
	if out.Replayed {
		t.Error("fresh call reported Replayed=true, want false")
	}
	if out.Status != StatusPending {
		t.Errorf("Status = %s, want pending (create-payment must not call the provider)", out.Status)
	}
	if got := mockProv.CallCounts().Charge; got != 0 {
		t.Errorf("provider Charge called %d times by CreatePayment, want 0", got)
	}

	var outboxCount int
	err = pool.QueryRow(context.Background(),
		`SELECT count(*) FROM outbox WHERE task_type = 'charge' AND status = 'pending' AND payload->>'payment_id' = $1`,
		out.PaymentID.String(),
	).Scan(&outboxCount)
	if err != nil {
		t.Fatalf("count outbox rows: %v", err)
	}
	if outboxCount != 1 {
		t.Errorf("pending outbox rows for this payment = %d, want 1", outboxCount)
	}
}

// TestService_ReplayOnSameKeySameBody is Phase 2 validation 2, adjusted for
// the async flow: hitting create-payment twice with the same
// Idempotency-Key and body must return the SAME "pending" acknowledgment
// both times (Replayed=true the second time) -- it does not reflect
// whatever ProcessCharge later does to the payment, since the idempotency
// record was completed with the pending response at HTTP time.
func TestService_ReplayOnSameKeySameBody(t *testing.T) {
	pool := testPool(t)
	mockProv := mock.New(mock.Config{Name: "mock"})
	svc := newTestService(pool, mockProv)

	tenantID := uuid.NewString()
	key := uuid.NewString()
	raw := bodyFor(tenantID, 1000)
	in := CreatePaymentInput{TenantID: tenantID, Currency: "USD", Amount: 1000, Provider: "mock"}

	first, err := svc.CreatePayment(context.Background(), in, key, raw)
	if err != nil {
		t.Fatalf("first CreatePayment: %v", err)
	}
	if first.Replayed {
		t.Fatal("first call reported Replayed=true, want false")
	}

	// Drive the payment to its final state via the worker path, same as
	// the real system would (asynchronously) -- proves the replayed
	// response still reports "pending", not the (by-then) real outcome.
	if err := svc.ProcessCharge(context.Background(), chargeInputFor(first.PaymentID, key, in)); err != nil {
		t.Fatalf("ProcessCharge: %v", err)
	}

	second, err := svc.CreatePayment(context.Background(), in, key, raw)
	if err != nil {
		t.Fatalf("second CreatePayment: %v", err)
	}
	if !second.Replayed {
		t.Error("second call with same key+body reported Replayed=false, want true")
	}
	if second.PaymentID != first.PaymentID {
		t.Errorf("second.PaymentID = %v, want %v (same payment)", second.PaymentID, first.PaymentID)
	}
	if second.Status != StatusPending {
		t.Errorf("replayed Status = %s, want pending (idempotency record stores the original 202 body)", second.Status)
	}
	if got := mockProv.CallCounts().Charge; got != 1 {
		t.Errorf("provider Charge called %d times, want exactly 1 (from the single ProcessCharge call)", got)
	}

	p, err := NewPGRepository(pool).Get(context.Background(), first.PaymentID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if p.Status != StatusSucceeded {
		t.Errorf("actual payment status = %s, want succeeded (mock defaults to succeeded)", p.Status)
	}
}

// TestService_ConflictOnSameKeyDifferentBody is Phase 2 validation 3: same
// Idempotency-Key, different body -> 409-mapped
// apperror.CodeIdempotencyConflict. CreatePayment never calls the provider
// regardless (conflict is detected at the idempotency-reserve stage, before
// any payment row exists for the second call).
func TestService_ConflictOnSameKeyDifferentBody(t *testing.T) {
	pool := testPool(t)
	mockProv := mock.New(mock.Config{Name: "mock"})
	svc := newTestService(pool, mockProv)

	tenantID := uuid.NewString()
	key := uuid.NewString()

	rawA := bodyFor(tenantID, 1000)
	inA := CreatePaymentInput{TenantID: tenantID, Currency: "USD", Amount: 1000, Provider: "mock"}
	if _, err := svc.CreatePayment(context.Background(), inA, key, rawA); err != nil {
		t.Fatalf("first CreatePayment: %v", err)
	}

	rawB := bodyFor(tenantID, 2000) // different amount -> different fingerprint
	inB := CreatePaymentInput{TenantID: tenantID, Currency: "USD", Amount: 2000, Provider: "mock"}
	_, err := svc.CreatePayment(context.Background(), inB, key, rawB)
	if err == nil {
		t.Fatal("CreatePayment with same key, different body = nil error, want conflict")
	}
	if apperror.CodeOf(err) != apperror.CodeIdempotencyConflict {
		t.Errorf("CodeOf(err) = %v, want %v", apperror.CodeOf(err), apperror.CodeIdempotencyConflict)
	}
	if got := mockProv.CallCounts().Charge; got != 0 {
		t.Errorf("provider Charge called %d times, want 0 (CreatePayment never calls the provider)", got)
	}
}

// TestService_ConcurrentCreatePaymentSameKey_OnlyOneReservationWins is
// Phase 2 validation 4, still meaningful post-Fase 3: N concurrent
// CreatePayment calls with the identical Idempotency-Key+body must result
// in exactly one winning reservation (one payment row, one outbox row) --
// losers get either a replayed pending response or
// CodeIdempotencyInProgress, never a second row.
func TestService_ConcurrentCreatePaymentSameKey_OnlyOneReservationWins(t *testing.T) {
	pool := testPool(t)
	mockProv := mock.New(mock.Config{Name: "mock"})
	svc := newTestService(pool, mockProv)

	tenantID := uuid.NewString()
	key := uuid.NewString()
	raw := bodyFor(tenantID, 500)
	in := CreatePaymentInput{TenantID: tenantID, Currency: "USD", Amount: 500, Provider: "mock"}

	const n = 8
	var wg sync.WaitGroup
	outs := make([]CreatePaymentOutput, n)
	errs := make([]error, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			outs[i], errs[i] = svc.CreatePayment(context.Background(), in, key, raw)
		}(i)
	}
	wg.Wait()

	if got := mockProv.CallCounts().Charge; got != 0 {
		t.Fatalf("provider Charge called %d times by concurrent CreatePayment calls, want 0", got)
	}

	var successCount int
	var paymentID uuid.UUID
	for i := range n {
		switch {
		case errs[i] == nil:
			successCount++
			if paymentID == uuid.Nil {
				paymentID = outs[i].PaymentID
			} else if outs[i].PaymentID != paymentID {
				t.Errorf("goroutine %d returned a different payment id: %v, want %v", i, outs[i].PaymentID, paymentID)
			}
		case apperror.CodeOf(errs[i]) == apperror.CodeIdempotencyInProgress:
			// Acceptable loser outcome: the winner hadn't completed yet
			// within the poll window.
		default:
			t.Errorf("goroutine %d returned unexpected error: %v", i, errs[i])
		}
	}
	if successCount == 0 {
		t.Fatal("no goroutine received a successful (possibly replayed) response")
	}

	var paymentCount int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM payments WHERE tenant_id = $1`, tenantID,
	).Scan(&paymentCount); err != nil {
		t.Fatalf("count payments: %v", err)
	}
	if paymentCount != 1 {
		t.Fatalf("payment rows for this tenant = %d, want exactly 1", paymentCount)
	}
}

// TestService_ProcessCharge_ConcurrentRedelivery_ChargeCalledOnce is the
// central Fase 3 invariant this phase adds (replacing the Phase 2 test that
// exercised the now-removed synchronous provider call inside
// CreatePayment): a charge task redelivered/duplicated N times for the SAME
// payment must result in exactly one provider.Charge call. This is what the
// "Catatan desain wajib" is about -- the pending->processing
// compare-and-swap in ProcessCharge, not anything in the queue layer, is
// what makes this true.
func TestService_ProcessCharge_ConcurrentRedelivery_ChargeCalledOnce(t *testing.T) {
	pool := testPool(t)
	mockProv := mock.New(mock.Config{Name: "mock"})
	svc := newTestService(pool, mockProv)

	tenantID := uuid.NewString()
	key := uuid.NewString()
	raw := bodyFor(tenantID, 700)
	in := CreatePaymentInput{TenantID: tenantID, Currency: "USD", Amount: 700, Provider: "mock"}

	out, err := svc.CreatePayment(context.Background(), in, key, raw)
	if err != nil {
		t.Fatalf("CreatePayment: %v", err)
	}

	chargeInput := chargeInputFor(out.PaymentID, key, in)

	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = svc.ProcessCharge(context.Background(), chargeInput)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("ProcessCharge redelivery %d returned error: %v (must return nil even for a losing CAS)", i, err)
		}
	}

	if got := mockProv.CallCounts().Charge; got != 1 {
		t.Fatalf("provider Charge called %d times across %d concurrent redeliveries, want exactly 1 (double charge)", got, n)
	}

	p, err := NewPGRepository(pool).Get(context.Background(), out.PaymentID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if p.Status != StatusSucceeded {
		t.Errorf("payment status = %s, want succeeded", p.Status)
	}
}

// TestService_ProcessCharge_ConcurrentRedeliveryWithLatency_ChargeCalledOnce
// is the same invariant as above, but with the mock provider configured to
// take 300ms per call -- long enough that the CAS contention window is wide
// open when every redelivery's ApplyTransition races. Without provider
// latency, it's plausible (though still not required to be) for the first
// redelivery to already be calling the provider before the others even
// reach the CAS, making the race less likely to actually be exercised;
// this forces it every run.
func TestService_ProcessCharge_ConcurrentRedeliveryWithLatency_ChargeCalledOnce(t *testing.T) {
	pool := testPool(t)
	mockProv := mock.New(mock.Config{Name: "mock", Latency: 300 * time.Millisecond})
	svc := newTestService(pool, mockProv)

	tenantID := uuid.NewString()
	key := uuid.NewString()
	raw := bodyFor(tenantID, 650)
	in := CreatePaymentInput{TenantID: tenantID, Currency: "USD", Amount: 650, Provider: "mock"}

	out, err := svc.CreatePayment(context.Background(), in, key, raw)
	if err != nil {
		t.Fatalf("CreatePayment: %v", err)
	}
	chargeInput := chargeInputFor(out.PaymentID, key, in)

	const n = 6
	var wg sync.WaitGroup
	var start sync.WaitGroup
	start.Add(1)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			start.Wait()
			errs[i] = svc.ProcessCharge(context.Background(), chargeInput)
		}(i)
	}
	start.Done()
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("ProcessCharge redelivery %d returned error: %v", i, err)
		}
	}

	if got := mockProv.CallCounts().Charge; got != 1 {
		t.Fatalf("provider Charge called %d times across %d concurrent redeliveries (with provider latency), want exactly 1", got, n)
	}
}

// TestService_ProcessCharge_ProviderTimeout_NoRetry_StaysProcessing is Fase
// 3 validation 4: a charge call that errors/times out must leave the
// payment "processing" (never retried automatically), and a SECOND
// ProcessCharge call for the same payment (simulating a naive extra worker
// attempt after the fact) must not call the provider again either -- by
// then the payment is no longer "pending", so the CAS rejects it.
func TestService_ProcessCharge_ProviderTimeout_NoRetry_StaysProcessing(t *testing.T) {
	pool := testPool(t)
	mockProv := mock.New(mock.Config{Name: "mock", ForceError: mock.ErrForced})
	svc := newTestService(pool, mockProv)

	tenantID := uuid.NewString()
	key := uuid.NewString()
	raw := bodyFor(tenantID, 900)
	in := CreatePaymentInput{TenantID: tenantID, Currency: "USD", Amount: 900, Provider: "mock"}

	out, err := svc.CreatePayment(context.Background(), in, key, raw)
	if err != nil {
		t.Fatalf("CreatePayment: %v", err)
	}
	chargeInput := chargeInputFor(out.PaymentID, key, in)

	if err := svc.ProcessCharge(context.Background(), chargeInput); err != nil {
		t.Fatalf("first ProcessCharge (provider error) returned error, want nil: %v", err)
	}
	if got := mockProv.CallCounts().Charge; got != 1 {
		t.Fatalf("provider Charge called %d times after first attempt, want 1", got)
	}

	repo := NewPGRepository(pool)
	p, err := repo.Get(context.Background(), out.PaymentID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if p.Status != StatusProcessing {
		t.Fatalf("payment status after provider error = %s, want processing (no auto-retry, no false failure)", p.Status)
	}

	// Simulate a redelivery (or a naive extra call) of the same task.
	if err := svc.ProcessCharge(context.Background(), chargeInput); err != nil {
		t.Fatalf("second ProcessCharge returned error, want nil: %v", err)
	}
	if got := mockProv.CallCounts().Charge; got != 1 {
		t.Fatalf("provider Charge called %d times after redelivery, want still 1 (no double charge)", got)
	}

	p, err = repo.Get(context.Background(), out.PaymentID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if p.Status != StatusProcessing {
		t.Fatalf("payment status after redelivery = %s, want still processing", p.Status)
	}
}

// TestService_ApplyProviderEvent_TerminalStateRejectsOlderEvent is Phase 2
// validation 5, adjusted: drive the payment to succeeded via ProcessCharge
// (not CreatePayment, which no longer produces a final outcome), then
// verify an older event trying to move it to failed is rejected and the
// stored state does not move.
func TestService_ApplyProviderEvent_TerminalStateRejectsOlderEvent(t *testing.T) {
	pool := testPool(t)
	repo := NewPGRepository(pool)
	mockProv := mock.New(mock.Config{Name: "mock", Status: provider.StatusSucceeded})
	svc := newTestService(pool, mockProv)

	tenantID := uuid.NewString()
	key := uuid.NewString()
	raw := bodyFor(tenantID, 750)
	in := CreatePaymentInput{TenantID: tenantID, Currency: "USD", Amount: 750, Provider: "mock"}

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
	if p.Status != StatusSucceeded {
		t.Fatalf("payment status = %s, want succeeded (mock configured to succeed)", p.Status)
	}

	olderEvent := provider.WebhookEvent{
		ProviderEventID: uuid.NewString(),
		ProviderRef:     *p.ProviderRef,
		Status:          provider.StatusFailed,
		OccurredAt:      time.Now().UTC().Add(-1 * time.Hour), // clearly before "now"
		RawPayload:      []byte(`{"late":true}`),
	}
	err = svc.ApplyProviderEvent(context.Background(), "mock", olderEvent)
	if err == nil {
		t.Fatal("ApplyProviderEvent with an older event on a terminal payment = nil, want rejection")
	}
	if apperror.CodeOf(err) != apperror.CodeTerminalState {
		t.Errorf("CodeOf(err) = %v, want %v", apperror.CodeOf(err), apperror.CodeTerminalState)
	}

	p, err = repo.Get(context.Background(), out.PaymentID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if p.Status != StatusSucceeded {
		t.Errorf("payment status after rejected event = %s, want it to remain succeeded (state must not move backwards)", p.Status)
	}
}

// TestService_ApplyProviderEvent_DuplicateProviderEventIDDropped is Phase 2
// validation 6, adjusted: drive the payment to "processing" (mock
// configured to stay processing) via ProcessCharge, then verify a
// provider_event_id applied once is dropped on redelivery.
func TestService_ApplyProviderEvent_DuplicateProviderEventIDDropped(t *testing.T) {
	pool := testPool(t)
	repo := NewPGRepository(pool)
	// Status Processing so the payment does NOT reach a terminal state via
	// ProcessCharge itself -- the webhook event is what drives it to
	// succeeded, isolating the dedup behavior from terminal-immutability.
	mockProv := mock.New(mock.Config{Name: "mock", Status: provider.StatusProcessing})
	svc := newTestService(pool, mockProv)

	tenantID := uuid.NewString()
	key := uuid.NewString()
	raw := bodyFor(tenantID, 300)
	in := CreatePaymentInput{TenantID: tenantID, Currency: "USD", Amount: 300, Provider: "mock"}

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
	if p.ProviderRef == nil {
		t.Fatal("provider_ref not set after ProcessCharge with a still-processing provider response")
	}

	eventID := uuid.NewString()
	evt := provider.WebhookEvent{
		ProviderEventID: eventID,
		ProviderRef:     *p.ProviderRef,
		Status:          provider.StatusSucceeded,
		OccurredAt:      time.Now().UTC(),
		RawPayload:      []byte(`{}`),
	}

	if err := svc.ApplyProviderEvent(context.Background(), "mock", evt); err != nil {
		t.Fatalf("first ApplyProviderEvent: %v", err)
	}

	err = svc.ApplyProviderEvent(context.Background(), "mock", evt) // identical event, redelivered
	if err == nil {
		t.Fatal("second ApplyProviderEvent with the same provider_event_id = nil, want it dropped")
	}
	if apperror.CodeOf(err) != apperror.CodeDuplicateEvent {
		t.Errorf("CodeOf(err) = %v, want %v", apperror.CodeOf(err), apperror.CodeDuplicateEvent)
	}

	p, err = repo.Get(context.Background(), out.PaymentID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if p.Status != StatusSucceeded {
		t.Errorf("payment status = %s, want succeeded (from the first, accepted event)", p.Status)
	}
}

// TestService_ProcessCharge_Bulkhead_SlowProviderDoesNotStarveFastProvider
// is Fase 3 validation 6, exercised at the layer the task actually
// describes: two mock providers behind the SAME Service/bulkhead, one
// configured slow, driven through the real ProcessCharge path (CAS +
// provider call + bulkhead acquire/release) rather than the bare
// bulkhead.Limiter in isolation. Saturating the slow provider's capacity
// must never delay a charge routed to the fast provider.
func TestService_ProcessCharge_Bulkhead_SlowProviderDoesNotStarveFastProvider(t *testing.T) {
	pool := testPool(t)
	repo := NewPGRepository(pool)
	idemStore := idempotency.NewPGStore(pool)

	slowProv := mock.New(mock.Config{Name: "slow", Latency: 2 * time.Second})
	fastProv := mock.New(mock.Config{Name: "fast"})
	registry := multiProviderRegistry{byName: map[string]provider.Provider{"slow": slowProv, "fast": fastProv}}

	svc := NewService(repo, idemStore, registry).WithBulkhead(bulkhead.New(2))

	// Saturate the "slow" provider's bulkhead capacity (2) with 2 charges
	// that will each block for ~2s inside prov.Charge.
	const slowCount = 2
	slowInputs := make([]ChargeTaskInput, slowCount)
	for i := 0; i < slowCount; i++ {
		tenantID := uuid.NewString()
		key := uuid.NewString()
		raw := bodyFor(tenantID, 100)
		in := CreatePaymentInput{TenantID: tenantID, Currency: "USD", Amount: 100, Provider: "slow"}
		out, err := svc.CreatePayment(context.Background(), in, key, raw)
		if err != nil {
			t.Fatalf("CreatePayment (slow #%d): %v", i, err)
		}
		slowInputs[i] = chargeInputFor(out.PaymentID, key, in)
	}

	var wg sync.WaitGroup
	for i := 0; i < slowCount; i++ {
		wg.Add(1)
		go func(in ChargeTaskInput) {
			defer wg.Done()
			if err := svc.ProcessCharge(context.Background(), in); err != nil {
				t.Errorf("ProcessCharge (slow): %v", err)
			}
		}(slowInputs[i])
	}

	// Give the slow goroutines a moment to actually acquire their bulkhead
	// slots and enter prov.Charge before measuring the fast provider.
	time.Sleep(200 * time.Millisecond)

	fastTenantID := uuid.NewString()
	fastKey := uuid.NewString()
	fastRaw := bodyFor(fastTenantID, 200)
	fastIn := CreatePaymentInput{TenantID: fastTenantID, Currency: "USD", Amount: 200, Provider: "fast"}
	fastOut, err := svc.CreatePayment(context.Background(), fastIn, fastKey, fastRaw)
	if err != nil {
		t.Fatalf("CreatePayment (fast): %v", err)
	}

	start := time.Now()
	if err := svc.ProcessCharge(context.Background(), chargeInputFor(fastOut.PaymentID, fastKey, fastIn)); err != nil {
		t.Fatalf("ProcessCharge (fast): %v", err)
	}
	elapsed := time.Since(start)

	wg.Wait()

	if elapsed > 500*time.Millisecond {
		t.Fatalf("ProcessCharge for the fast provider took %v while the slow provider's bulkhead was saturated -- bulkhead did not isolate providers", elapsed)
	}
	if got := fastProv.CallCounts().Charge; got != 1 {
		t.Errorf("fast provider Charge calls = %d, want 1", got)
	}
	if got := slowProv.CallCounts().Charge; got != slowCount {
		t.Errorf("slow provider Charge calls = %d, want %d", got, slowCount)
	}
}
