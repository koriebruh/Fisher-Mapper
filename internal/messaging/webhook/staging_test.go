package webhook

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"Fisher-Mapper/internal/domain/apperror"
	"Fisher-Mapper/internal/provider"
)

// testPool is gated on TEST_POSTGRES_DSN, matching the convention used by
// internal/domain/payment's integration tests -- migrations 00001-00003
// must already be applied.
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

// insertTestPayment inserts a minimal real payments row and returns its id
// -- incoming_webhook_events.payment_id has a foreign key to payments, so
// Join/MarkProcessed tests need a real row to reference, not a fabricated
// uuid.
func insertTestPayment(t *testing.T, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	const insertSQL = `
		INSERT INTO payments (tenant_id, livemode, currency, amount, operation_type, provider, status)
		VALUES ($1, false, 'USD', 100, 'charge', 'mock', 'pending')
		RETURNING id`
	if err := pool.QueryRow(context.Background(), insertSQL, uuid.NewString()).Scan(&id); err != nil {
		t.Fatalf("insert test payment: %v", err)
	}
	return id
}

// TestStore_Stage_DedupsByProviderEventIDProviderRef is the Fase 3
// mandatory unit test: "staging dedup by (provider, event_id,
// provider_ref)". Staging the identical triple twice must result in
// exactly one row.
func TestStore_Stage_DedupsByProviderEventIDProviderRef(t *testing.T) {
	pool := testPool(t)
	store := NewStore(pool)
	ctx := context.Background()

	providerName := "mock"
	eventID := uuid.NewString()
	providerRef := uuid.NewString()
	payload := []byte(`{"a":1}`)

	if err := store.Stage(ctx, providerName, eventID, providerRef, payload); err != nil {
		t.Fatalf("first Stage: %v", err)
	}
	if err := store.Stage(ctx, providerName, eventID, providerRef, payload); err != nil {
		t.Fatalf("second Stage (duplicate): %v", err)
	}

	var count int
	err := pool.QueryRow(ctx,
		`SELECT count(*) FROM incoming_webhook_events WHERE provider=$1 AND event_id=$2 AND provider_ref=$3`,
		providerName, eventID, providerRef,
	).Scan(&count)
	if err != nil {
		t.Fatalf("count staged rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("staged row count = %d, want exactly 1 (dedup on provider+event_id+provider_ref)", count)
	}

	// A different event_id for the same provider_ref must NOT be
	// deduplicated -- dedup is scoped to the full triple, not provider_ref
	// alone.
	otherEventID := uuid.NewString()
	if err := store.Stage(ctx, providerName, otherEventID, providerRef, payload); err != nil {
		t.Fatalf("Stage with different event_id: %v", err)
	}
	events, err := store.FindUnprocessedByRef(ctx, providerName, providerRef)
	if err != nil {
		t.Fatalf("FindUnprocessedByRef: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("unprocessed events for provider_ref = %d, want 2 (one per distinct event_id)", len(events))
	}
}

// TestJoin_AppliesStagedEventOnceMatchingPaymentExists is Fase 3 validation
// 2's second half: a webhook staged before any payment row existed for its
// provider_ref must get applied once Join is called with that provider_ref
// -- exercised by calling Join directly, as the plan explicitly sanctions
// for deterministic testing.
func TestJoin_AppliesStagedEventOnceMatchingPaymentExists(t *testing.T) {
	pool := testPool(t)
	store := NewStore(pool)
	ctx := context.Background()

	providerName := "mock"
	providerRef := uuid.NewString()
	eventID := uuid.NewString()
	payload := []byte(fmt.Sprintf(`{"provider_event_id":%q,"provider_ref":%q,"status":"succeeded"}`, eventID, providerRef))

	if err := store.Stage(ctx, providerName, eventID, providerRef, payload); err != nil {
		t.Fatalf("Stage: %v", err)
	}

	paymentID := insertTestPayment(t, pool)
	var appliedWith provider.WebhookEvent
	var applyCalls int

	parse := func(_ context.Context, raw []byte) (provider.WebhookEvent, error) {
		return provider.WebhookEvent{
			ProviderEventID: eventID,
			ProviderRef:     providerRef,
			Status:          provider.StatusSucceeded,
		}, nil
	}
	apply := func(_ context.Context, name string, evt provider.WebhookEvent) error {
		applyCalls++
		appliedWith = evt
		return nil
	}

	resolved, err := Join(ctx, store, parse, apply, providerName, providerRef, paymentID)
	if err != nil {
		t.Fatalf("Join: %v", err)
	}
	if resolved != 1 {
		t.Fatalf("Join resolved = %d, want 1", resolved)
	}
	if applyCalls != 1 {
		t.Fatalf("apply called %d times, want exactly 1", applyCalls)
	}
	if appliedWith.ProviderRef != providerRef {
		t.Errorf("applied event ProviderRef = %q, want %q", appliedWith.ProviderRef, providerRef)
	}

	// A second Join call must not re-apply: the event is now marked
	// processed.
	resolved, err = Join(ctx, store, parse, apply, providerName, providerRef, paymentID)
	if err != nil {
		t.Fatalf("second Join: %v", err)
	}
	if resolved != 0 {
		t.Fatalf("second Join resolved = %d, want 0 (already processed)", resolved)
	}
	if applyCalls != 1 {
		t.Fatalf("apply called %d times after second Join, want still 1", applyCalls)
	}
}

// TestJoin_LeavesGenuineApplyErrorUnprocessed verifies a real (non-benign)
// apply error does not mark the staged event processed, so a later Join
// call gets another chance.
func TestJoin_LeavesGenuineApplyErrorUnprocessed(t *testing.T) {
	pool := testPool(t)
	store := NewStore(pool)
	ctx := context.Background()

	providerName := "mock"
	providerRef := uuid.NewString()
	eventID := uuid.NewString()

	if err := store.Stage(ctx, providerName, eventID, providerRef, []byte(`{}`)); err != nil {
		t.Fatalf("Stage: %v", err)
	}

	parse := func(_ context.Context, _ []byte) (provider.WebhookEvent, error) {
		return provider.WebhookEvent{ProviderEventID: eventID, ProviderRef: providerRef, Status: provider.StatusSucceeded}, nil
	}
	apply := func(_ context.Context, _ string, _ provider.WebhookEvent) error {
		return apperror.Wrap(apperror.CodeInternal, "boom", errors.New("db exploded"))
	}

	resolved, err := Join(ctx, store, parse, apply, providerName, providerRef, uuid.New())
	if err != nil {
		t.Fatalf("Join: %v", err)
	}
	if resolved != 0 {
		t.Fatalf("resolved = %d, want 0 (genuine error must not be marked resolved)", resolved)
	}

	events, err := store.FindUnprocessedByRef(ctx, providerName, providerRef)
	if err != nil {
		t.Fatalf("FindUnprocessedByRef: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("unprocessed events = %d, want 1 (still pending retry)", len(events))
	}
}
