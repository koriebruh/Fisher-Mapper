// Package lifecycle wires long-running processes as oklog/run actors so
// every actor (fiber server, later: grpc server, asynq worker, outbox
// relay poller, dynamic-config watcher) shuts down deterministically on
// SIGINT/SIGTERM. There is no "go func()" without an owning actor.
package lifecycle

import (
	"context"
	"time"

	"github.com/gofiber/fiber/v2"
)

// FiberActor returns the (execute, interrupt) pair for run.Group.Add,
// running app on addr and shutting it down gracefully (bounded by
// shutdownTimeout) when interrupted.
func FiberActor(app *fiber.App, addr string, shutdownTimeout time.Duration) (execute func() error, interrupt func(error)) {
	execute = func() error {
		return app.Listen(addr)
	}
	interrupt = func(error) {
		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		_ = app.ShutdownWithContext(ctx)
	}
	return execute, interrupt
}
