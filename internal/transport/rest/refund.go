package rest

import (
	"encoding/json"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"

	"Fisher-Mapper/internal/apperror"
	"Fisher-Mapper/internal/domain/payment"
)

// createRefundRequest is the wire shape for POST /payments/:id/refunds.
type createRefundRequest struct {
	Currency string `json:"currency"`
	Amount   int64  `json:"amount"`
}

// RegisterRefundRoutes registers the Fase 4 refund endpoints on the same
// PaymentDeps the charge endpoints use (same Service, same "satu service
// layer" principle).
func RegisterRefundRoutes(router fiber.Router, deps PaymentDeps) {
	router.Post("/:id/refunds", handleCreateRefund(deps))
	router.Get("/refunds/:id", handleGetRefund(deps))
}

func handleCreateRefund(deps PaymentDeps) fiber.Handler {
	return func(c *fiber.Ctx) error {
		paymentID, err := uuid.Parse(c.Params("id"))
		if err != nil {
			return writeError(c, apperror.New(apperror.CodeValidation, "invalid payment id"))
		}

		idempotencyKey := c.Get("Idempotency-Key")
		raw := append([]byte(nil), c.Body()...)

		var req createRefundRequest
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &req); err != nil {
				return writeError(c, apperror.New(apperror.CodeValidation, "invalid JSON body"))
			}
		}

		p, err := deps.Service.GetPayment(c.Context(), paymentID)
		if err != nil {
			return writeError(c, err)
		}

		input := payment.CreateRefundInput{
			PaymentID: paymentID,
			TenantID:  p.TenantID,
			Livemode:  p.Livemode,
			Currency:  req.Currency,
			Amount:    req.Amount,
		}

		out, err := deps.Service.CreateRefund(c.Context(), input, idempotencyKey, raw)
		if err != nil {
			return writeError(c, err)
		}

		status := fiber.StatusAccepted
		if out.Replayed {
			status = fiber.StatusOK
		}
		return c.Status(status).JSON(out)
	}
}

// refundView is the wire shape for GET /payments/refunds/:id.
type refundView struct {
	RefundID          uuid.UUID `json:"refund_id"`
	PaymentID         uuid.UUID `json:"payment_id"`
	Status            string    `json:"status"`
	ProviderRefundRef string    `json:"provider_refund_ref,omitempty"`
}

func handleGetRefund(deps PaymentDeps) fiber.Handler {
	return func(c *fiber.Ctx) error {
		id, err := uuid.Parse(c.Params("id"))
		if err != nil {
			return writeError(c, apperror.New(apperror.CodeValidation, "invalid refund id"))
		}

		r, err := deps.Service.GetRefund(c.Context(), id)
		if err != nil {
			return writeError(c, err)
		}

		view := refundView{RefundID: r.ID, PaymentID: r.PaymentID, Status: string(r.Status)}
		if r.ProviderRefundRef != nil {
			view.ProviderRefundRef = *r.ProviderRefundRef
		}
		return c.JSON(view)
	}
}
