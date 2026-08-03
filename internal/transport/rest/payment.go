package rest

import (
	"encoding/json"

	"github.com/gofiber/fiber/v2"

	"Fisher-Mapper/internal/apperror"
	"Fisher-Mapper/internal/domain/payment"
)

// PaymentDeps are the dependencies the payment route group needs. Kept
// separate from the top-level Deps struct so payment.go has no reason to
// know about Pool/QueueClient.
type PaymentDeps struct {
	Service *payment.Service
}

// createPaymentRequest is the wire shape for POST /payments. Field names
// intentionally mirror the domain's CreatePaymentInput.
type createPaymentRequest struct {
	TenantID      string            `json:"tenant_id"`
	Livemode      bool              `json:"livemode"`
	Currency      string            `json:"currency"`
	Amount        int64             `json:"amount"`
	Provider      string            `json:"provider"`
	PaymentMethod string            `json:"payment_method"`
	Metadata      map[string]string `json:"metadata"`
}

// RegisterPaymentRoutes registers the Phase 2 payment endpoint.
//
// This handler is intentionally thin: request decode -> call into
// payment.Service -> map the result/error to an HTTP response. All
// business logic (idempotency, provider call, state transition) lives in
// the service, per the plan's "satu service layer, dua transport tipis"
// principle — the same service will back a gRPC handler in Phase 6.
func RegisterPaymentRoutes(app *fiber.App, deps PaymentDeps) {
	app.Post("/payments", handleCreatePayment(deps))
}

func handleCreatePayment(deps PaymentDeps) fiber.Handler {
	return func(c *fiber.Ctx) error {
		idempotencyKey := c.Get("Idempotency-Key")

		// Copy the body: fiber may reuse/reset its internal buffer after
		// the handler returns, and this byte slice is also what gets
		// hashed for the idempotency fingerprint — it must stay stable for
		// the lifetime of the service call.
		raw := append([]byte(nil), c.Body()...)

		var req createPaymentRequest
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &req); err != nil {
				return writeError(c, apperror.New(apperror.CodeValidation, "invalid JSON body"))
			}
		}

		input := payment.CreatePaymentInput{
			TenantID:      req.TenantID,
			Livemode:      req.Livemode,
			Currency:      req.Currency,
			Amount:        req.Amount,
			Provider:      req.Provider,
			PaymentMethod: req.PaymentMethod,
			Metadata:      req.Metadata,
		}

		out, err := deps.Service.CreatePayment(c.Context(), input, idempotencyKey, raw)
		if err != nil {
			return writeError(c, err)
		}

		status := fiber.StatusCreated
		if out.Replayed {
			status = fiber.StatusOK
		}
		return c.Status(status).JSON(out)
	}
}
