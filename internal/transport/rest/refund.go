package rest

import (
	"encoding/json"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"

	"Fisher-Mapper/internal/domain/apperror"
	"Fisher-Mapper/internal/domain/payment"
)

type createRefundRequest struct {
	Currency string `json:"currency"`
	Amount   int64  `json:"amount"`

	// SourceApp/Description mirror createPaymentRequest's fields -- see
	// payment.Envelope's doc.
	SourceApp   *string `json:"source_app,omitempty"`
	Description *string `json:"description,omitempty"`
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

		p, err := deps.Service.GetPayment(c.UserContext(), paymentID)
		if err != nil {
			return writeError(c, err)
		}

		input := payment.CreateRefundInput{
			PaymentID: paymentID,
			TenantID:  p.TenantID,
			Livemode:  p.Livemode,
			Currency:  req.Currency,
			Amount:    req.Amount,
			Envelope:  buildEnvelope(c, req.SourceApp, req.Description),
		}

		out, err := deps.Service.CreateRefund(c.UserContext(), input, idempotencyKey, raw)
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

type refundView struct {
	RefundID          uuid.UUID `json:"refund_id"`
	PaymentID         uuid.UUID `json:"payment_id"`
	Status            string    `json:"status"`
	ProviderRefundRef string    `json:"provider_refund_ref,omitempty"`

	// Envelope fields, exposed verbatim -- mirrors paymentView.
	SourceApp        string `json:"source_app,omitempty"`
	Channel          string `json:"channel"`
	TraceID          string `json:"trace_id,omitempty"`
	Description      string `json:"description,omitempty"`
	InitiatedBy      string `json:"initiated_by"`
	RequestIP        string `json:"request_ip,omitempty"`
	RequestUserAgent string `json:"request_user_agent,omitempty"`
}

func newRefundView(r *payment.Refund) refundView {
	view := refundView{
		RefundID:    r.ID,
		PaymentID:   r.PaymentID,
		Status:      string(r.Status),
		Channel:     r.Channel,
		InitiatedBy: r.InitiatedBy,
	}
	if r.ProviderRefundRef != nil {
		view.ProviderRefundRef = *r.ProviderRefundRef
	}
	if r.SourceApp != nil {
		view.SourceApp = *r.SourceApp
	}
	if r.TraceID != nil {
		view.TraceID = *r.TraceID
	}
	if r.Description != nil {
		view.Description = *r.Description
	}
	if r.RequestIP != nil {
		view.RequestIP = *r.RequestIP
	}
	if r.RequestUserAgent != nil {
		view.RequestUserAgent = *r.RequestUserAgent
	}
	return view
}

func handleGetRefund(deps PaymentDeps) fiber.Handler {
	return func(c *fiber.Ctx) error {
		id, err := uuid.Parse(c.Params("id"))
		if err != nil {
			return writeError(c, apperror.New(apperror.CodeValidation, "invalid refund id"))
		}

		r, err := deps.Service.GetRefund(c.UserContext(), id)
		if err != nil {
			return writeError(c, err)
		}

		return c.JSON(newRefundView(r))
	}
}
