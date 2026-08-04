package queue

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisHealthChecker caches a Redis reachability check for interval,
// re-probing only after it expires. This is what lets SwitchingClient
// re-evaluate live (not just once at boot) whether to dispatch through
// asynq or fall back to the in-process memory client -- Fase 3 validation 3
// requires the relay to notice Redis coming back without a restart.
type RedisHealthChecker struct {
	client   *redis.Client
	interval time.Duration

	mu        sync.Mutex
	lastCheck time.Time
	healthy   bool
	checked   bool
}

// NewRedisHealthChecker builds a checker against addr, using its own Redis
// connection (separate from the asynq client) so a health probe never
// contends with task dispatch traffic.
func NewRedisHealthChecker(addr string, interval time.Duration) *RedisHealthChecker {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	return &RedisHealthChecker{
		client:   redis.NewClient(&redis.Options{Addr: addr}),
		interval: interval,
	}
}

// Healthy reports whether Redis was reachable as of the last check,
// re-probing (bounded by a short timeout derived from ctx) if the cached
// result has expired.
func (h *RedisHealthChecker) Healthy(ctx context.Context) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.checked && time.Since(h.lastCheck) < h.interval {
		return h.healthy
	}

	pingCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	h.healthy = h.client.Ping(pingCtx).Err() == nil
	h.lastCheck = time.Now()
	h.checked = true
	return h.healthy
}

func (h *RedisHealthChecker) Close() error {
	return h.client.Close()
}
