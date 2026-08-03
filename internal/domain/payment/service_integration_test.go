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

	"Fisher-Mapper/internal/apperror"
	"Fisher-Mapper/internal/idempotency"
	"Fisher-Mapper/internal/provider"
	"Fisher-Mapper/internal/provider/mock"
)

// These tests exercise the real Postgres-backed Repository + idempotency
// Store together — the atomic-insert race (validation 4), replay/conflict
// (validations 2/3), and state-machine/dedup enforcement under a real row
// lock (validations 5/6) can't be verified against a fake in-memory
// repository, since the whole point is the DB-level guarantee.
//
// Gated on TEST_POSTGRES_DSN so `go test ./...` stays green without a
// running Postgres; migrations 00001+00002 must already be applied (run
// `make migrate-up` against the same DSN, e.g. via docker-compose, before
// running these).
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
// interface with one fixed provider, regardless of the requested name —
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

func bodyFor(tenantID string, amount int64) []byte {
	return []byte(fmt.Sprintf(`{"tenant_id":%q,"currency":"USD","amount":%d,"provider":"mock"}`, tenantID, amount))
}

// TestService_CreatePayment_ReplayOnSameKeySameBody is Phase 2 validation
// 2: hitting create-payment twice with the same Idempotency-Key and the
// same body must return the first call's response (Replayed=true), and
// must not call the provider a second time.
func TestService_CreatePayment_ReplayOnSameKeySameBody(t *testing.T) {
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
	if got := mockProv.CallCounts().Charge; got != 1 {
		t.Errorf("provider Charge called %d times, want exactly 1 (second request must not re-process)", got)
	}
}

// TestService_CreatePayment_ConflictOnSameKeyDifferentBody is Phase 2
// validation 3: same Idempotency-Key, different body -> 409-mapped
// apperror.CodeIdempotencyConflict, and the provider must not be called a
// second time either.
func TestService_CreatePayment_ConflictOnSameKeyDifferentBody(t *testing.T) {
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
	if got := mockProv.CallCounts().Charge; got != 1 {
		t.Errorf("provider Charge called %d times, want exactly 1 (conflicting request must not call provider)", got)
	}
}

// TestService_CreatePayment_ConcurrentSameKey_ChargeCalledOnce is Phase 2
// validation 4: two goroutines racing with the identical Idempotency-Key
// and body must result in exactly one provider.Charge call — the DB's
// atomic insert on (tenant_id, idempotency_key), not "check then insert",
// is what makes this true. Run with `go test -race`.
func TestService_CreatePayment_ConcurrentSameKey_ChargeCalledOnce(t *testing.T) {
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

	if got := mockProv.CallCounts().Charge; got != 1 {
		t.Fatalf("provider Charge called %d times across %d concurrent requests, want exactly 1 (double charge)", got, n)
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
			// within the poll window. Still not a double charge.
		default:
			t.Errorf("goroutine %d returned unexpected error: %v", i, errs[i])
		}
	}
	if successCount == 0 {
		t.Fatal("no goroutine received a successful (possibly replayed) response")
	}
}

// TestService_CreatePayment_ConcurrentSameKeyWithProviderLatency exercises
// the same race as the test above, but with the mock provider configured
// to take 300ms per call — long enough that every "loser" goroutine is
// guaranteed to observe idempotency.StateInProgress (the reservation row
// exists but Complete hasn't run yet) rather than finding it already
// StateCompleted. Without this, an instant mock provider makes it likely
// every loser lands on StateCompleted and the StateInProgress poll branch
// in Service.pollForCompletion never actually executes under contention —
// this test forces that branch to run and still requires exactly one
// Charge call and one converged payment id.
func TestService_CreatePayment_ConcurrentSameKeyWithProviderLatency(t *testing.T) {
	pool := testPool(t)
	mockProv := mock.New(mock.Config{Name: "mock", Latency: 300 * time.Millisecond})
	svc := newTestService(pool, mockProv)

	tenantID := uuid.NewString()
	key := uuid.NewString()
	raw := bodyFor(tenantID, 600)
	in := CreatePaymentInput{TenantID: tenantID, Currency: "USD", Amount: 600, Provider: "mock"}

	const n = 6
	var wg sync.WaitGroup
	outs := make([]CreatePaymentOutput, n)
	errs := make([]error, n)

	var start sync.WaitGroup
	start.Add(1)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			start.Wait()
			outs[i], errs[i] = svc.CreatePayment(context.Background(), in, key, raw)
		}(i)
	}
	start.Done()
	wg.Wait()

	if got := mockProv.CallCounts().Charge; got != 1 {
		t.Fatalf("provider Charge called %d times across %d concurrent requests (with provider latency), want exactly 1", got, n)
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
			// A loser that gave up waiting inside the poll window — still
			// not a double charge, and this is the branch this test exists
			// to force.
		default:
			t.Errorf("goroutine %d returned unexpected error: %v", i, errs[i])
		}
	}
	if successCount == 0 {
		t.Fatal("no goroutine received a successful (possibly replayed) response")
	}
}

// TestService_ApplyProviderEvent_TerminalStateRejectsOlderEvent is Phase 2
// validation 5: once a payment reaches succeeded, an event with an OLDER
// timestamp trying to move it to failed must be rejected, and the stored
// state must not move.
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
	if out.Status != StatusSucceeded {
		t.Fatalf("payment status = %s, want succeeded (mock configured to succeed)", out.Status)
	}

	olderEvent := provider.WebhookEvent{
		ProviderEventID: uuid.NewString(),
		ProviderRef:     out.ProviderRef,
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

	p, err := repo.Get(context.Background(), out.PaymentID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if p.Status != StatusSucceeded {
		t.Errorf("payment status after rejected event = %s, want it to remain succeeded (state must not move backwards)", p.Status)
	}
}

// TestService_ApplyProviderEvent_DuplicateProviderEventIDDropped is Phase
// 2 validation 6: an event with a provider_event_id already applied must
// be dropped on the second delivery, not re-applied.
func TestService_ApplyProviderEvent_DuplicateProviderEventIDDropped(t *testing.T) {
	pool := testPool(t)
	repo := NewPGRepository(pool)
	// Status Processing so the payment does NOT reach a terminal state via
	// CreatePayment itself — the webhook event is what drives it to
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
	if out.Status != StatusProcessing {
		t.Fatalf("payment status = %s, want processing", out.Status)
	}

	eventID := uuid.NewString()
	evt := provider.WebhookEvent{
		ProviderEventID: eventID,
		ProviderRef:     out.ProviderRef,
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

	p, err := repo.Get(context.Background(), out.PaymentID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if p.Status != StatusSucceeded {
		t.Errorf("payment status = %s, want succeeded (from the first, accepted event)", p.Status)
	}
}
