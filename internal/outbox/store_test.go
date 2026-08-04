package outbox

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// testPool is gated on TEST_POSTGRES_DSN, matching the convention used
// elsewhere in this module -- migrations 00001-00003 must already be
// applied.
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

// cleanOutbox removes every existing row so each test starts from a known
// state -- this table is shared across this package's tests AND with
// internal/domain/payment's integration tests (which insert real 'charge'
// rows via CreateWithOutbox against the same docker-compose database), so
// without this, leftover pending rows from an earlier test file's run would
// be claimed here too and throw off exact-count assertions.
func cleanOutbox(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `DELETE FROM outbox`); err != nil {
		t.Fatalf("clean outbox: %v", err)
	}
}

func insertRows(t *testing.T, pool *pgxpool.Pool, n int) []uuid.UUID {
	t.Helper()
	ids := make([]uuid.UUID, 0, n)
	for i := 0; i < n; i++ {
		id, err := Insert(context.Background(), pool, "test_task", []byte(`{}`))
		if err != nil {
			t.Fatalf("insert row %d: %v", i, err)
		}
		ids = append(ids, id)
	}
	return ids
}

// TestClaim_ConcurrentPollersNeverClaimTheSameRow is the Fase 3 mandatory
// unit test: "outbox claim (dua goroutine poll barengan gak dapet row
// sama)". Structured with an explicit barrier (tx A opens and claims, and
// is deliberately held open before committing) so tx B's claim is
// guaranteed to run while A's rows are still locked -- without that, a
// naive two-goroutine test can pass "by accident" if B simply runs after A
// has already committed.
func TestClaim_ConcurrentPollersNeverClaimTheSameRow(t *testing.T) {
	pool := testPool(t)
	cleanOutbox(t, pool)
	ctx := context.Background()
	ids := insertRows(t, pool, 6)

	txA, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx A: %v", err)
	}
	defer txA.Rollback(ctx) //nolint:errcheck

	rowsA, err := claim(ctx, txA, 3)
	if err != nil {
		t.Fatalf("claim A: %v", err)
	}
	if len(rowsA) != 3 {
		t.Fatalf("claim A got %d rows, want 3", len(rowsA))
	}

	// txA is still open (not committed/rolled back) -- its 3 claimed rows
	// remain FOR UPDATE locked. A second, independent transaction polling
	// concurrently must skip them entirely.
	txB, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx B: %v", err)
	}
	defer txB.Rollback(ctx) //nolint:errcheck

	rowsB, err := claim(ctx, txB, 10) // ask for more than the remaining 3
	if err != nil {
		t.Fatalf("claim B: %v", err)
	}
	if len(rowsB) != 3 {
		t.Fatalf("claim B got %d rows, want exactly the 3 unlocked rows (SKIP LOCKED)", len(rowsB))
	}

	seen := make(map[uuid.UUID]bool)
	for _, r := range rowsA {
		seen[r.ID] = true
	}
	for _, r := range rowsB {
		if seen[r.ID] {
			t.Fatalf("row %s claimed by both transaction A and B -- SKIP LOCKED violated", r.ID)
		}
		seen[r.ID] = true
	}
	if len(seen) != len(ids) {
		t.Fatalf("total distinct rows claimed = %d, want %d", len(seen), len(ids))
	}

	if err := txA.Commit(ctx); err != nil {
		t.Fatalf("commit A: %v", err)
	}
	if err := txB.Commit(ctx); err != nil {
		t.Fatalf("commit B: %v", err)
	}
}

// TestClaim_ConcurrentGoroutines is a supplementary real-concurrency
// version of the same guarantee using actual goroutines racing via
// DispatchBatch (which commits internally), asserting every row is
// dispatched exactly once across both.
func TestClaim_ConcurrentGoroutines(t *testing.T) {
	pool := testPool(t)
	cleanOutbox(t, pool)
	store := NewStore(pool)
	ctx := context.Background()
	ids := insertRows(t, pool, 20)

	var mu sync.Mutex
	seen := make(map[uuid.UUID]int)
	dispatch := func(_ context.Context, row Row) error {
		mu.Lock()
		seen[row.ID]++
		mu.Unlock()
		return nil
	}

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _, err := store.DispatchBatch(ctx, 20, dispatch)
			if err != nil {
				t.Errorf("DispatchBatch: %v", err)
			}
		}()
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != len(ids) {
		t.Fatalf("dispatched %d distinct rows, want %d", len(seen), len(ids))
	}
	for id, count := range seen {
		if count != 1 {
			t.Errorf("row %s dispatched %d times, want exactly 1", id, count)
		}
	}
}
