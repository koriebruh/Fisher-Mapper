package queue

import (
	"context"
	"log/slog"
)

// SwitchingClient dispatches through AsynqClient while Redis is healthy and
// falls back to MemoryClient the moment it isn't -- re-checked on every
// Enqueue via health (cached/bounded by RedisHealthChecker's interval), not
// just once at process start. This is the Client the outbox relay is wired
// against; the HTTP create-payment path never touches it (or Redis) at all
// -- it only ever writes to Postgres.
type SwitchingClient struct {
	asynqClient  *AsynqClient
	memoryClient *MemoryClient
	health       *RedisHealthChecker
}

func NewSwitchingClient(asynqClient *AsynqClient, memoryClient *MemoryClient, health *RedisHealthChecker) *SwitchingClient {
	return &SwitchingClient{asynqClient: asynqClient, memoryClient: memoryClient, health: health}
}

func (c *SwitchingClient) Enqueue(ctx context.Context, taskType string, payload []byte, opts EnqueueOptions) error {
	if c.health.Healthy(ctx) {
		if err := c.asynqClient.Enqueue(ctx, taskType, payload, opts); err != nil {
			// Health said "up" a moment ago but the actual call failed
			// (e.g. Redis died in between). Leave the outbox row pending
			// by surfacing the error -- do NOT silently fall back to
			// memory here, or a task could be dispatched twice (once
			// half-attempted via asynq, once via memory) for what the
			// caller believes was one dispatch. The next relay tick will
			// re-check health and pick correctly.
			slog.Warn("queue: asynq enqueue failed despite healthy check, leaving for retry", "error", err, "task_type", taskType)
			return err
		}
		return nil
	}
	return c.memoryClient.Enqueue(ctx, taskType, payload, opts)
}

func (c *SwitchingClient) Close() error {
	err1 := c.asynqClient.Close()
	err2 := c.memoryClient.Close()
	err3 := c.health.Close()
	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}
	return err3
}

var _ Client = (*SwitchingClient)(nil)
