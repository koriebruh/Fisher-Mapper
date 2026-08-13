package rest

import (
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"

	"Fisher-Mapper/internal/domain/apperror"
	"Fisher-Mapper/internal/domain/payment"
	"Fisher-Mapper/internal/messaging/webhook"
	"Fisher-Mapper/internal/provider"
	"Fisher-Mapper/internal/provider/auth"
)

// WebhookDeps are the dependencies POST /webhooks/:provider needs.
type WebhookDeps struct {
	Providers *provider.Registry
	Verifiers map[string]auth.Verifier
	Staging   *webhook.Store
	Service   *payment.Service
}

// RegisterWebhookRoutes registers the Fase 3 inbound webhook endpoint.
//
// Per plan Decide Now item 9: this handler must NEVER 404 a provider_ref
// that has no payment row yet -- it stages the event and acknowledges 200
// so the PJP stops retrying. It also always applies-or-stages BEFORE
// deciding the HTTP status, so a webhook that races ahead of our own
// create-payment transaction is never lost.
func RegisterWebhookRoutes(app *fiber.App, deps WebhookDeps) {
	app.Post("/webhooks/:provider", handleWebhook(deps))
}

func handleWebhook(deps WebhookDeps) fiber.Handler {
	return func(c *fiber.Ctx) error {
		ctx := c.Context()
		providerName := c.Params("provider")

		prov, err := deps.Providers.Get(providerName)
		if err != nil {
			return writeError(c, err)
		}

		raw := append([]byte(nil), c.Body()...)

		evt, err := prov.ParseWebhook(ctx, provider.ParseWebhookRequest{
			Headers: headerMap(c),
			Body:    raw,
		})
		if err != nil {
			return writeError(c, apperror.Wrap(apperror.CodeValidation, "webhook: malformed payload", err))
		}

		verifier, ok := deps.Verifiers[providerName]
		if !ok {
			return writeError(c, apperror.New(apperror.CodeUnauthorized, "webhook: no verifier configured for this provider"))
		}

		ts := parseUnixHeader(c.Get("X-Timestamp"))
		verifyErr := verifier.Verify(ctx, auth.VerifyInput{
			Method:          c.Method(),
			Path:            c.Path(),
			Body:            raw,
			Timestamp:       ts,
			Signature:       c.Get("X-Signature"),
			Provider:        providerName,
			ProviderEventID: evt.ProviderEventID,
		})
		if verifyErr != nil {
			if apperror.CodeOf(verifyErr) == apperror.CodeDuplicateEvent {
				// Already-seen event (checked against payment_events, i.e.
				// a payment row exists and this event was already applied
				// to it): ack and drop, nothing left to do.
				return c.SendStatus(fiber.StatusOK)
			}
			return writeError(c, verifyErr)
		}

		applyErr := deps.Service.ApplyProviderEvent(ctx, providerName, evt)
		switch {
		case applyErr == nil:
			return c.SendStatus(fiber.StatusOK)

		case apperror.CodeOf(applyErr) == apperror.CodeNotFound:
			// The whole point of Decide Now item 9: no payment row yet for
			// this provider_ref. Stage it -- never 404 -- and ack 200 so
			// the PJP stops retrying. payment.Service.ProcessCharge's
			// webhook.Join call re-applies this once the payment row (and
			// its provider_ref) exists.
			if stageErr := deps.Staging.Stage(ctx, providerName, evt.ProviderEventID, evt.ProviderRef, raw); stageErr != nil {
				return writeError(c, stageErr)
			}
			return c.SendStatus(fiber.StatusOK)

		case apperror.CodeOf(applyErr) == apperror.CodeDuplicateEvent,
			apperror.CodeOf(applyErr) == apperror.CodeStaleEvent,
			apperror.CodeOf(applyErr) == apperror.CodeTerminalState:
			// Benign no-op outcomes from the payment's perspective (already
			// applied / out of order / payment already terminal) -- still
			// ack 200, nothing to stage or retry.
			return c.SendStatus(fiber.StatusOK)

		default:
			// Genuine unexpected error (DB down, etc) -- let the PJP's
			// normal retry semantics apply.
			return writeError(c, applyErr)
		}
	}
}

func headerMap(c *fiber.Ctx) map[string]string {
	headers := make(map[string]string)
	c.Request().Header.VisitAll(func(key, value []byte) {
		headers[string(key)] = string(value)
	})
	return headers
}

func parseUnixHeader(v string) time.Time {
	if v == "" {
		return time.Time{}
	}
	sec, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.Unix(sec, 0).UTC()
}
