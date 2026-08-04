// Package ratelimit is a "stub cheap" item from the project plan: an
// in-memory token-bucket rate limiter, exposed as a fiber middleware keyed
// per client/API key. Deliberately simple (no distributed state -- each
// process has its own buckets) since the plan only calls for a stub here,
// not a production-grade distributed limiter.
package ratelimit

import (
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
)

// Limiter is a token-bucket rate limiter with independent buckets per key.
type Limiter struct {
	ratePerSecond float64
	burst         float64
	now           func() time.Time

	mu      sync.Mutex
	buckets map[string]*bucket
}

type bucket struct {
	tokens float64
	last   time.Time
}

// New builds a Limiter allowing ratePerSecond tokens/sec to refill, up to
// burst tokens banked per key.
func New(ratePerSecond, burst float64) *Limiter {
	if ratePerSecond <= 0 {
		ratePerSecond = 1
	}
	if burst <= 0 {
		burst = ratePerSecond
	}
	return &Limiter{
		ratePerSecond: ratePerSecond,
		burst:         burst,
		now:           time.Now,
		buckets:       make(map[string]*bucket),
	}
}

// Allow reports whether one token is available for key, consuming it if so.
func (l *Limiter) Allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	b, ok := l.buckets[key]
	if !ok {
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[key] = b
	}

	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * l.ratePerSecond
		if b.tokens > l.burst {
			b.tokens = l.burst
		}
		b.last = now
	}

	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

// KeyFunc extracts the rate-limit bucket key from a request. Middleware
// falls back to c.IP() if this returns "".
type KeyFunc func(c *fiber.Ctx) string

// APIKeyOrIP is the default KeyFunc: the X-Api-Key header if present,
// otherwise the caller's IP.
func APIKeyOrIP(c *fiber.Ctx) string {
	if k := c.Get("X-Api-Key"); k != "" {
		return k
	}
	return c.IP()
}

// Middleware builds a fiber.Handler that rejects requests exceeding l's
// rate with 429, keyed by keyFunc (APIKeyOrIP if nil).
func Middleware(l *Limiter, keyFunc KeyFunc) fiber.Handler {
	if keyFunc == nil {
		keyFunc = APIKeyOrIP
	}
	return func(c *fiber.Ctx) error {
		key := keyFunc(c)
		if key == "" {
			key = c.IP()
		}
		if !l.Allow(key) {
			return c.Status(fiber.StatusTooManyRequests).JSON(fiber.Map{
				"error": fiber.Map{
					"code":    "rate_limited",
					"message": "too many requests",
				},
			})
		}
		return c.Next()
	}
}
