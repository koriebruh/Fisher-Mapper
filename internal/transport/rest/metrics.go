package rest

import (
	"time"

	"github.com/gofiber/fiber/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"Fisher-Mapper/internal/platform/observability"
)

// RegisterMetricsMiddleware wires the Fase 5 HTTP-request-latency histogram
// as its own middleware -- deliberately NOT reusing otelfiber's built-in
// metrics module (disabled via otelfiber.WithoutMetrics in NewApp), so this
// process owns the exact instrument name/attributes/unit recorded here
// rather than depending on a third-party package's semconv choices.
func RegisterMetricsMiddleware(app *fiber.App, metrics *observability.Metrics) {
	app.Use(func(c *fiber.Ctx) error {
		start := time.Now()
		err := c.Next()

		route := c.Route().Path
		if route == "" {
			route = c.Path()
		}
		metrics.HTTPRequestDuration.Record(c.UserContext(), time.Since(start).Seconds(),
			metric.WithAttributes(
				attribute.String("method", c.Method()),
				attribute.String("route", route),
				attribute.Int("status_code", c.Response().StatusCode()),
			),
		)
		return err
	})
}
