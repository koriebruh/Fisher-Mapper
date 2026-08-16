// Package payment is the payment domain: the Payment aggregate, its
// explicit state machine, the repository interface (and pgx
// implementation) that persists it under row-level locking, and the
// service that ties provider calls + idempotency + state transitions
// together for the REST (and later gRPC) transport layer to call into.
//
// Per plan: "satu service layer, dua transport tipis" — all business logic
// lives here, never in a transport handler.
package payment

import (
	"time"

	"github.com/google/uuid"

	"Fisher-Mapper/internal/provider/payload"
)

// OperationType is locked in by plan "Invarian Uang — Decide Now" item 4:
// not just "charge" — charge/authorize/capture/refund/payout/reversal from
// day one, matching the CHECK constraint in migration 00001.
type OperationType string

const (
	OperationCharge    OperationType = "charge"
	OperationAuthorize OperationType = "authorize"
	OperationCapture   OperationType = "capture"
	OperationRefund    OperationType = "refund"
	OperationPayout    OperationType = "payout"
	OperationReversal  OperationType = "reversal"
)

// Envelope carries the request/actor metadata a real financial transaction
// record needs beyond the money invariants themselves -- embedded
// (anonymously) into Payment/Refund/Payout so all three operation types
// carry the SAME fields, populated the same way, per the task's explicit
// "payout must carry the SAME completeness as charge/refund" requirement.
//
// Every field here has a real setter (a transport or the service layer, at
// creation time) and a real reader (GetPayment/GetRefund/GetPayout's REST
// JSON and gRPC responses) -- none of this is scaffolding.
type Envelope struct {
	// SourceApp is a client-supplied identifier for WHICH calling
	// application originated the request -- this template may back
	// multiple client apps/channels, and this is "which one", distinct from
	// Channel ("how" it called). Nullable: optional, the caller may not set
	// it.
	SourceApp *string

	// Channel is set by the TRANSPORT layer itself ("rest" / "grpc"), never
	// caller-supplied -- a client cannot claim to be a different transport
	// than the one it actually used.
	Channel string

	// TraceID is the request-level correlation identifier, captured from
	// the ACTIVE span at creation time (nil when otel_enabled=false or no
	// valid span exists -- an all-zeros trace id is worse than no trace id
	// on a financial record). Lets a payment row surviving after the trace
	// exporter's own data ages out stay traceable to its originating
	// request.
	TraceID *string

	// Description is a free-text merchant reference/memo (e.g. "invoice
	// #1234", "order ref ABC") -- common in real PJP APIs, flows through to
	// statements/receipts. Caller-supplied, optional.
	Description *string

	// InitiatedBy is the actor that initiated the original create request:
	// "customer" (every creation path today), "system" (reserved -- no
	// system-driven creation path exists yet), or "admin" (reserved for a
	// future manual/admin-initiated creation path). See payment_events'
	// per-transition InitiatedBy (TransitionParams) for the transition-level
	// analogue of this same taxonomy.
	InitiatedBy string

	// RequestIP / RequestUserAgent are captured from the transport layer at
	// creation time (REST: fiber.Ctx; gRPC: peer/metadata, best-effort) --
	// nullable, since not every transport can supply them.
	RequestIP        *string
	RequestUserAgent *string
}

// Payment is the domain aggregate backing the `payments` table.
type Payment struct {
	ID            uuid.UUID
	TenantID      string
	Livemode      bool
	Currency      string
	Amount        int64
	OperationType OperationType
	Provider      string
	ProviderRef   *string

	// PaymentMethod is the caller-requested category ("qris"/
	// "virtual_account"/"card"/...), forwarded to provider.ChargeRequest and
	// persisted here so a caller can see back which method a charge actually
	// used -- it used to flow through to the provider and get silently
	// dropped on the way back. May be empty ("") for a method-agnostic
	// charge.
	PaymentMethod string

	// MethodPayload is the method-specific data the provider returned
	// alongside the charge (a QRIS string, VA number, card redirect, ...) --
	// see internal/provider/payload. Nil until ProcessCharge persists the
	// provider's response (which may happen while still "processing" -- a
	// QRIS string is typically returned before scan-to-pay completes).
	MethodPayload *payload.MethodPayload

	// CallbackURL is a caller-supplied best-effort delivery target notified
	// once this payment reaches a terminal status (see
	// Service.WithCallbackNotifier) -- nil when the caller didn't set one.
	CallbackURL *string

	Status      Status
	LastEventAt time.Time
	CreatedAt   time.Time
	UpdatedAt   time.Time
	Envelope
}

// Refund is the domain aggregate backing the `refunds` table (Fase 4, plan
// Decide Now item 10). It has its OWN Status instance running through the
// exact same pending->processing->succeeded|failed graph (Transition,
// statemachine.go) as Payment — but as a separate row/lifecycle: a Payment
// stays "succeeded" forever even while one or more Refunds against it move
// through their own states.
type Refund struct {
	ID        uuid.UUID
	PaymentID uuid.UUID
	TenantID  string
	Livemode  bool
	Currency  string
	Amount    int64
	// OperationType is always OperationRefund -- kept as a real column/field
	// (rather than a hardcoded literal only in SQL) so refunds is
	// consistent with payments/payouts, all three of which expose
	// operation_type per the plan's money-invariant list.
	OperationType     OperationType
	Provider          string
	ProviderRef       *string // the ORIGINAL charge's provider_ref, needed by provider.RefundRequest
	ProviderRefundRef *string // the reference the provider assigns to this refund itself
	Status            Status
	LastEventAt       time.Time
	CreatedAt         time.Time
	UpdatedAt         time.Time
	Envelope
}

// Payout is the domain aggregate backing the `payouts` table: money OUT,
// standalone -- independent of any prior charge, unlike Refund. Its own
// state machine instance runs through the identical pending->processing->
// succeeded|failed graph (Transition, statemachine.go).
type Payout struct {
	ID          uuid.UUID
	TenantID    string
	Livemode    bool
	Currency    string
	Amount      int64
	Provider    string
	ProviderRef *string // the reference THIS provider assigns to the payout call itself
	// Destination is an opaque identifier for where the funds go (a
	// provider-side bank-account/e-wallet token, never raw bank/card
	// account data) -- see provider.PayoutRequest.Destination.
	Destination string

	// CallbackURL mirrors Payment.CallbackURL's doc exactly -- a
	// caller-supplied best-effort delivery target notified once this payout
	// reaches a terminal status. Nil when the caller didn't set one.
	CallbackURL *string

	Status      Status
	LastEventAt time.Time
	CreatedAt   time.Time
	UpdatedAt   time.Time
	Envelope
}
