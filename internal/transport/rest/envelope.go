package rest

import (
	"github.com/gofiber/fiber/v2"
	"go.opentelemetry.io/otel/trace"

	"Fisher-Mapper/internal/domain/payment"
)

// buildEnvelope populates a payment.Envelope from a REST request the same
// way for every creation endpoint (payment/refund/payout): sourceApp and
// description are the two caller-supplied optional fields the request body
// carries, everything else (Channel, InitiatedBy, TraceID, RequestIP,
// RequestUserAgent) is derived by this transport itself, never taken from
// the client.
func buildEnvelope(c *fiber.Ctx, sourceApp, description *string) payment.Envelope {
	env := payment.Envelope{
		SourceApp: sourceApp,
		// Every REST creation call today is a direct client hitting this
		// API on behalf of itself, never this template acting on its own --
		// "customer" per Envelope.InitiatedBy's taxonomy.
		Channel:     payment.ChannelREST,
		Description: description,
		InitiatedBy: payment.InitiatedByCustomer,
	}

	if ip := c.IP(); ip != "" {
		env.RequestIP = &ip
	}
	if ua := c.Get("User-Agent"); ua != "" {
		env.RequestUserAgent = &ua
	}

	// otelfiber's middleware (see health.go's NewApp) stores the per-request
	// span in c.UserContext() -- an all-zeros trace id (no valid span, e.g.
	// otel_enabled=false) is worse than no trace id on a financial record,
	// so this stays nil rather than recording a meaningless value.
	if sc := trace.SpanContextFromContext(c.UserContext()); sc.IsValid() {
		id := sc.TraceID().String()
		env.TraceID = &id
	}

	return env
}
