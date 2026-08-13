// Package lifecycle wires long-running processes as oklog/run actors so
// every actor (fiber server, later: grpc server, asynq worker, outbox
// relay poller, dynamic-config watcher) shuts down deterministically on
// SIGINT/SIGTERM. There is no "go func()" without an owning actor.
package lifecycle

import (
	"context"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/hibiken/asynq"
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

// RunnerActor adapts any long-running func(ctx) error — the outbox relay's
// Run method, in particular — into an (execute, interrupt) pair for
// run.Group.Add. interrupt cancels the context passed to run; execute
// returns once run does (which, for a well-behaved runner, is as soon as it
// observes ctx.Done()).
func RunnerActor(run func(ctx context.Context) error) (execute func() error, interrupt func(error)) {
	ctx, cancel := context.WithCancel(context.Background())
	execute = func() error {
		return run(ctx)
	}
	interrupt = func(error) {
		cancel()
	}
	return execute, interrupt
}

// AsynqServerActor adapts an *asynq.Server + handler into an (execute,
// interrupt) pair for run.Group.Add. asynq.Server has its own graceful stop
// (Shutdown, no context) rather than a ctx-cancel shape, so this is a
// dedicated adapter instead of going through RunnerActor.
func AsynqServerActor(srv *asynq.Server, handler asynq.Handler) (execute func() error, interrupt func(error)) {
	execute = func() error {
		return srv.Run(handler)
	}
	interrupt = func(error) {
		srv.Shutdown()
	}
	return execute, interrupt
}
