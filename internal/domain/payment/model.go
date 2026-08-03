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
	Status        Status
	LastEventAt   time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
}
