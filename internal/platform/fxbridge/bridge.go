// Package fxbridge adapts internal/platform/lifecycle's (execute, interrupt)
// actor pairs -- the shape every actor in this repo already returns for
// oklog/run.Group.Add -- into fx.Hook, so cmd/server and cmd/worker can
// register the same actor constructors under fx.Lifecycle instead of
// run.Group, with no changes to internal/platform/lifecycle itself.
package fxbridge

import (
	"context"

	"go.uber.org/fx"
)

// Bridge appends a hook that starts execute in a goroutine on OnStart and
// calls interrupt on OnStop. OnStart must return immediately (fx blocks
// startup on it), so execute -- normally blocking, e.g. app.Listen -- cannot
// run inline. If execute returns a non-nil error (the actor crashed rather
// than shutting down cleanly), sh.Shutdown reports it to fx so the whole app
// exits with a non-zero code, matching oklog/run.Group's "any actor's
// execute() returning stops every actor" behavior.
func Bridge(lc fx.Lifecycle, sh fx.Shutdowner, execute func() error, interrupt func(error)) {
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go func() {
				if err := execute(); err != nil {
					_ = sh.Shutdown(fx.ExitCode(1))
				}
			}()
			return nil
		},
		OnStop: func(context.Context) error {
			interrupt(nil)
			return nil
		},
	})
}
