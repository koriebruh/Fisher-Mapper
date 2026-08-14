package rest

import (
	"encoding/json"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"

	"Fisher-Mapper/internal/domain/apperror"
	"Fisher-Mapper/internal/domain/payment"
)

// createPayoutRequest is the wire shape for POST /payouts -- a standalone
// money-OUT operation, unlike refunds (which hang off an existing
// payment): Provider and Destination are caller-supplied here, not derived
// from a parent row.
type createPayoutRequest struct {
	TenantID    string            `json:"tenant_id"`
	Livemode    bool              `json:"livemode"`
	Currency    string            `json:"currency"`
	Amount      int64             `json:"amount"`
	Provider    string            `json:"provider"`
	Destination string            `json:"destination"`
	Metadata    map[string]string `json:"metadata"`
}

// RegisterPayoutRoutes registers the payout endpoints on router -- same thin
// handler / single-service-layer principle as RegisterPaymentRoutes.
func RegisterPayoutRoutes(router fiber.Router, deps PaymentDeps) {
	router.Post("/", handleCreatePayout(deps))
	router.Get("/:id", handleGetPayout(deps))
}

func handleCreatePayout(deps PaymentDeps) fiber.Handler {
	return func(c *fiber.Ctx) error {
		idempotencyKey := c.Get("Idempotency-Key")

		raw := append([]byte(nil), c.Body()...)

		var req createPayoutRequest
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &req); err != nil {
				return writeError(c, apperror.New(apperror.CodeValidation, "invalid JSON body"))
			}
		}

		input := payment.CreatePayoutInput{
			TenantID:    req.TenantID,
			Livemode:    req.Livemode,
			Currency:    req.Currency,
			Amount:      req.Amount,
			Provider:    req.Provider,
			Destination: req.Destination,
			Metadata:    req.Metadata,
		}

		out, err := deps.Service.CreatePayout(c.UserContext(), input, idempotencyKey, raw)
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

type payoutView struct {
	PayoutID    uuid.UUID `json:"payout_id"`
	Status      string    `json:"status"`
	ProviderRef string    `json:"provider_ref,omitempty"`
}

func handleGetPayout(deps PaymentDeps) fiber.Handler {
	return func(c *fiber.Ctx) error {
		id, err := uuid.Parse(c.Params("id"))
		if err != nil {
			return writeError(c, apperror.New(apperror.CodeValidation, "invalid payout id"))
		}

		p, err := deps.Service.GetPayout(c.UserContext(), id)
		if err != nil {
			return writeError(c, err)
		}

		view := payoutView{PayoutID: p.ID, Status: string(p.Status)}
		if p.ProviderRef != nil {
			view.ProviderRef = *p.ProviderRef
		}
		return c.JSON(view)
	}
}
