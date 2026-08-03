// Package queue owns the asynq client connection. Task/worker logic is out
// of scope for Phase 1 — this is connect-and-ping only, per the walking
// skeleton plan.
package queue

import (
	"fmt"

	"github.com/hibiken/asynq"
)

// NewClient creates an asynq client pointed at the given Redis address.
// Task enqueue/worker wiring is added in a later phase.
func NewClient(addr string) *asynq.Client {
	return asynq.NewClient(asynq.RedisClientOpt{Addr: addr})
}

// Ping verifies connectivity to Redis through the asynq client.
func Ping(client *asynq.Client) error {
	if err := client.Ping(); err != nil {
		return fmt.Errorf("queue: ping redis: %w", err)
	}
	return nil
}
