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
//
// Note: this table is shared with internal/domain/payment's integration
// tests, which insert real 'charge' rows via CreateWithOutbox against the
// same docker-compose database. Under plain `go test ./...`, Go runs
// different packages' test binaries in separate, concurrent processes, so
// this package's claim calls can observe extra pending rows inserted by the
// payment package mid-test. Deliberately NOT truncating the table to work
// around this (that would corrupt whatever the payment package's own
// concurrently-running test is doing) -- instead, every assertion below is
// membership-based against the specific rows THIS test inserted, so extra
// rows from elsewhere are tolerated rather than causing flakiness. (`go
// test ./... -p 1` also avoids the cross-package interleaving entirely, if
// preferred -- see the Makefile's `test-db` target.)
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
//
// Both claims use a large limit so, regardless of how many other pending
// rows exist system-wide (see package doc above), everything currently
// pending gets split across A and B -- assertions then check membership
// only for the rows this test itself inserted, plus the global "no row
// claimed by both" invariant across everything either transaction saw.
func TestClaim_ConcurrentPollersNeverClaimTheSameRow(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	ids := insertRows(t, pool, 6)
	want := make(map[uuid.UUID]bool, len(ids))
	for _, id := range ids {
		want[id] = true
	}

	txA, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx A: %v", err)
	}
	defer txA.Rollback(ctx) //nolint:errcheck

	rowsA, err := claim(ctx, txA, 1000)
	if err != nil {
		t.Fatalf("claim A: %v", err)
	}

	// txA is still open (not committed/rolled back) -- its claimed rows
	// remain FOR UPDATE locked. A second, independent transaction polling
	// concurrently must skip them entirely.
	txB, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx B: %v", err)
	}
	defer txB.Rollback(ctx) //nolint:errcheck

	rowsB, err := claim(ctx, txB, 1000)
	if err != nil {
		t.Fatalf("claim B: %v", err)
	}

	seenBy := make(map[uuid.UUID]int)
	for _, r := range rowsA {
		seenBy[r.ID]++
	}
	for _, r := range rowsB {
		seenBy[r.ID]++
		if seenBy[r.ID] > 1 {
			t.Fatalf("row %s claimed by both transaction A and B -- SKIP LOCKED violated", r.ID)
		}
	}

	for id := range want {
		if seenBy[id] == 0 {
			t.Errorf("row %s (inserted by this test) was not claimed by either transaction", id)
		}
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
// DispatchBatch (which commits internally), asserting every row THIS test
// inserted is dispatched exactly once, and that nothing (including any
// extra row from elsewhere, per the package doc above) is ever dispatched
// more than once.
func TestClaim_ConcurrentGoroutines(t *testing.T) {
	pool := testPool(t)
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

	for _, id := range ids {
		if seen[id] != 1 {
			t.Errorf("row %s (inserted by this test) dispatched %d times, want exactly 1", id, seen[id])
		}
	}
	for id, count := range seen {
		if count > 1 {
			t.Errorf("row %s dispatched %d times, want at most 1 (double dispatch)", id, count)
		}
	}
}
