// Package lifecycle wires long-running processes as oklog/run actors so
// every actor (fiber server, grpc server, asynq worker, outbox relay
// poller, dynamic-config watcher) shuts down deterministically on
// SIGINT/SIGTERM. There is no "go func()" without an owning actor.
package lifecycle

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/hibiken/asynq"
	"google.golang.org/grpc"
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

// HTTPServerActor adapts a plain *http.Server into an (execute, interrupt)
// pair for run.Group.Add -- the Fase 5 metrics-endpoint listener in
// cmd/worker, which has no fiber.App of its own (unlike cmd/server, whose
// /metrics route is just another fiber route -- see FiberActor).
func HTTPServerActor(srv *http.Server, shutdownTimeout time.Duration) (execute func() error, interrupt func(error)) {
	execute = func() error {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
	interrupt = func(error) {
		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}
	return execute, interrupt
}

// GRPCServerActor returns the (execute, interrupt) pair for run.Group.Add,
// serving srv on the already-bound lis (bind it BEFORE calling g.Run() --
// creating the listener here, inside execute, would turn a bad port/address
// into an async failure the run.Group only observes after every other actor
// has already started).
//
// interrupt calls GracefulStop in a goroutine bounded by shutdownTimeout
// (same shutdown-timeout shape as FiberActor), falling back to a hard Stop
// if it doesn't finish in time -- GracefulStop on its own blocks until every
// in-flight RPC completes, with no timeout of its own, which would prevent
// the process from ever exiting on an interrupt with a stuck call in
// flight.
func GRPCServerActor(srv *grpc.Server, lis net.Listener, shutdownTimeout time.Duration) (execute func() error, interrupt func(error)) {
	execute = func() error {
		if err := srv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			return err
		}
		return nil
	}
	interrupt = func(error) {
		stopped := make(chan struct{})
		go func() {
			srv.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(shutdownTimeout):
			srv.Stop()
		}
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
