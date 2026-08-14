package ratelimit

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
)

func TestLimiter_AllowsUpToBurstThenBlocks(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	l := New(1, 3) // 1 token/sec, burst 3
	l.now = func() time.Time { return now }

	for i := 0; i < 3; i++ {
		if !l.Allow("k") {
			t.Fatalf("Allow() call %d within burst = false, want true", i+1)
		}
	}
	if l.Allow("k") {
		t.Fatal("Allow() beyond burst = true, want false")
	}
}

func TestLimiter_RefillsOverTime(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	l := New(1, 1) // 1 token/sec, burst 1
	l.now = func() time.Time { return now }

	if !l.Allow("k") {
		t.Fatal("first Allow() = false, want true")
	}
	if l.Allow("k") {
		t.Fatal("immediate second Allow() = true, want false (bucket empty)")
	}

	now = now.Add(1100 * time.Millisecond)
	if !l.Allow("k") {
		t.Fatal("Allow() after refill window = false, want true")
	}
}

func TestLimiter_KeysAreIndependent(t *testing.T) {
	l := New(1, 1)
	if !l.Allow("a") {
		t.Fatal("Allow(a) = false, want true")
	}
	if !l.Allow("b") {
		t.Fatal("Allow(b) should be independent of key a's consumption")
	}
}

// newTestApp wires Middleware exactly like rest.NewApp does, with a fixed
// key (client identity doesn't matter for these two tests).
func newTestApp(l *Limiter, enabled func() bool) *fiber.App {
	app := fiber.New()
	app.Use(Middleware(l, func(*fiber.Ctx) string { return "k" }, enabled))
	app.Get("/", func(c *fiber.Ctx) error { return c.SendStatus(fiber.StatusOK) })
	return app
}

// TestMiddleware_EnabledTrueRejectsBeyondBurst proves the dynamic-config
// ratelimit.enabled=true path still rejects, matching pre-toggle behavior.
func TestMiddleware_EnabledTrueRejectsBeyondBurst(t *testing.T) {
	app := newTestApp(New(1, 1), func() bool { return true })

	if resp := doGet(t, app); resp.StatusCode != http.StatusOK {
		t.Fatalf("first request status = %d, want 200", resp.StatusCode)
	}
	resp := doGet(t, app)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("second request status = %d, want 429 (enabled=true, burst exhausted)", resp.StatusCode)
	}
}

// TestMiddleware_EnabledFalseBypassesLimiter proves the toggle actually
// changes behavior: the exact same 1-burst Limiter that rejects the second
// request above lets an unbounded number of requests through once enabled
// reports false.
func TestMiddleware_EnabledFalseBypassesLimiter(t *testing.T) {
	app := newTestApp(New(1, 1), func() bool { return false })

	for i := 0; i < 3; i++ {
		resp := doGet(t, app)
		if resp.StatusCode != http.StatusOK {
			t.Errorf("request %d status = %d, want 200 (ratelimit disabled)", i, resp.StatusCode)
		}
	}
}

func doGet(t *testing.T, app *fiber.App) *http.Response {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	return resp
}
