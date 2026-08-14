package rest

import (
	"encoding/json"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"

	"Fisher-Mapper/internal/domain/apperror"
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

	// SourceApp/Description are the two caller-supplied Envelope fields --
	// see payment.Envelope's doc. Both optional.
	SourceApp   *string `json:"source_app,omitempty"`
	Description *string `json:"description,omitempty"`
}

// RegisterPaymentRoutes registers the payment endpoints on router (either
// the top-level *fiber.App or a sub-group, e.g. one with rate-limit
// middleware applied — see transport/rest/health.go's NewApp).
//
// This handler is intentionally thin: request decode -> call into
// payment.Service -> map the result/error to an HTTP response. All
// business logic (idempotency, provider call, state transition) lives in
// the service, per the plan's "satu service layer, dua transport tipis"
// principle — the same service will back a gRPC handler in Phase 6.
func RegisterPaymentRoutes(router fiber.Router, deps PaymentDeps) {
	router.Post("/", handleCreatePayment(deps))
	router.Get("/:id", handleGetPayment(deps))
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
			Envelope:      buildEnvelope(c, req.SourceApp, req.Description),
		}

		// UserContext (not Context): otelfiber's tracing middleware stores
		// the per-request span there via c.SetUserContext -- the service
		// call, and everything it does transitively (outbox insert, trace
		// carrier injection), must run under that context for the create
		// -> outbox -> worker trace to connect (Fase 5 item 5).
		out, err := deps.Service.CreatePayment(c.UserContext(), input, idempotencyKey, raw)
		if err != nil {
			return writeError(c, err)
		}

		// Fase 3 addendum: create-payment is async now -- a fresh
		// (non-replayed) call never has a final outcome to report yet, so
		// it is always 202 Accepted. A replayed response (same
		// key+fingerprint as an earlier call) is 200 OK, same as Phase 2.
		status := fiber.StatusAccepted
		if out.Replayed {
			status = fiber.StatusOK
		}
		return c.Status(status).JSON(out)
	}
}

// paymentView is the wire shape for GET /payments/{id} -- the read side of
// the async flow, letting a caller poll for the outcome CreatePayment's
// 202 response didn't include.
type paymentView struct {
	PaymentID   uuid.UUID `json:"payment_id"`
	Status      string    `json:"status"`
	ProviderRef string    `json:"provider_ref,omitempty"`

	// Envelope fields, exposed verbatim -- see payment.Envelope's doc for
	// what each one means.
	SourceApp        string `json:"source_app,omitempty"`
	Channel          string `json:"channel"`
	TraceID          string `json:"trace_id,omitempty"`
	Description      string `json:"description,omitempty"`
	InitiatedBy      string `json:"initiated_by"`
	RequestIP        string `json:"request_ip,omitempty"`
	RequestUserAgent string `json:"request_user_agent,omitempty"`
}

// newPaymentView copies p's envelope pointer fields into their
// omitempty-friendly plain-string equivalents -- shared shape for both
// direct GET responses and, once populated the same way, any future list
// endpoint.
func newPaymentView(p *payment.Payment) paymentView {
	view := paymentView{
		PaymentID:   p.ID,
		Status:      string(p.Status),
		Channel:     p.Channel,
		InitiatedBy: p.InitiatedBy,
	}
	if p.ProviderRef != nil {
		view.ProviderRef = *p.ProviderRef
	}
	if p.SourceApp != nil {
		view.SourceApp = *p.SourceApp
	}
	if p.TraceID != nil {
		view.TraceID = *p.TraceID
	}
	if p.Description != nil {
		view.Description = *p.Description
	}
	if p.RequestIP != nil {
		view.RequestIP = *p.RequestIP
	}
	if p.RequestUserAgent != nil {
		view.RequestUserAgent = *p.RequestUserAgent
	}
	return view
}

func handleGetPayment(deps PaymentDeps) fiber.Handler {
	return func(c *fiber.Ctx) error {
		id, err := uuid.Parse(c.Params("id"))
		if err != nil {
			return writeError(c, apperror.New(apperror.CodeValidation, "invalid payment id"))
		}

		p, err := deps.Service.GetPayment(c.UserContext(), id)
		if err != nil {
			return writeError(c, err)
		}

		return c.JSON(newPaymentView(p))
	}
}
