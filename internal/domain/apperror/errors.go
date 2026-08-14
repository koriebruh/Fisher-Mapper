// Package apperror defines a small, transport-agnostic error taxonomy
// shared across the domain/service layer. Transports (REST now, gRPC in
// Phase 6) map Code to their own status representation (HTTP status,
// gRPC status code) at the edge — the domain/service layer never imports
// a transport package to report an error.
package apperror

import (
	"errors"
	"fmt"
)

// Code identifies a category of error. Kept as a small, stable, string enum
// so it is easy to log, compare, and map per-transport without a switch on
// error message text.
type Code string

const (
	// CodeValidation marks a caller input problem (missing/invalid field).
	CodeValidation Code = "validation_error"

	// CodeIdempotencyConflict: same Idempotency-Key, different request
	// fingerprint. Per plan: "Key sama+fingerprint beda = 409".
	CodeIdempotencyConflict Code = "idempotency_conflict"

	// CodeIdempotencyInProgress: same key+fingerprint, but the request that
	// reserved it has not completed yet (concurrent request race).
	CodeIdempotencyInProgress Code = "idempotency_in_progress"

	// CodeProviderTimeout: the PJP call did not complete in time. Per plan
	// item 12 ("Charge = no-auto-retry"), this must never trigger an
	// automatic retry at this layer.
	CodeProviderTimeout Code = "provider_timeout"

	// CodeProviderError: the PJP call failed for a reason other than
	// timeout (rejected, 4xx/5xx from provider, etc).
	CodeProviderError Code = "provider_error"

	// CodeProviderNotRegistered: the requested provider name has no
	// registry entry.
	CodeProviderNotRegistered Code = "provider_not_registered"

	// CodeNotFound: referenced entity (payment, ...) does not exist.
	CodeNotFound Code = "not_found"

	// CodeInvalidTransition: the requested state transition is not part of
	// the allowed pending -> processing -> succeeded|failed graph.
	CodeInvalidTransition Code = "invalid_transition"

	// CodeTerminalState: attempted to transition a payment already in a
	// terminal state (succeeded/failed). Terminal states are immutable.
	CodeTerminalState Code = "terminal_state"

	// CodeStaleEvent: the incoming event's timestamp is older than the
	// timestamp of the last event already applied to this payment. Must be
	// rejected without moving state backwards.
	CodeStaleEvent Code = "stale_event"

	// CodeDuplicateEvent: an event with the same (provider, provider_event_id)
	// was already applied. Must be dropped, not re-applied.
	CodeDuplicateEvent Code = "duplicate_event"

	// CodeUnauthorized: inbound webhook/auth verification failed (bad
	// signature, timestamp outside the allowed skew window).
	CodeUnauthorized Code = "unauthorized"

	// CodeProviderDisabled: the dynamic-config provider-enabled flag was off
	// for this provider at the time a charge/refund task was picked up.
	// Fase 4 mitigation, checked both at outbox-relay dispatch time and
	// again immediately before the provider call inside
	// payment.Service.ProcessCharge/ProcessRefund. Returned (not swallowed
	// as nil) when caught BEFORE the pending->processing CAS, so the task
	// lands in terminal_failures with its full payload intact — replayable
	// once the provider is re-enabled — rather than leaving the payment
	// stuck "processing" with no provider_ref to reconcile against.
	CodeProviderDisabled Code = "provider_disabled"

	// CodeRefundLimitExceeded: sum(refunds already pending/processing/
	// succeeded for a payment) + this refund's amount would exceed the
	// original charge amount. Plan Decide Now item 10.
	CodeRefundLimitExceeded Code = "refund_limit_exceeded"

	// CodeInternal: anything else (unexpected DB error, etc).
	CodeInternal Code = "internal"
)

// Source categorizes WHO is responsible for an error, for structured
// logging: a human reading an incident log line must be able to tell "is
// this our bug, a bad caller request, or the PJP/an upstream dependency
// misbehaving" without reading source code. See SourceOf.
type Source string

const (
	// SourceClient: the caller sent a bad request (validation failure,
	// idempotency conflict/replay race, referencing something that does not
	// exist, a state transition the caller asked for that isn't valid).
	SourceClient Source = "client"

	// SourceInternal: a bug or bad state in THIS codebase (misconfiguration,
	// an invariant this template itself is supposed to guarantee having
	// been violated, an unexpected DB write failure).
	SourceInternal Source = "internal"

	// SourceProvider: the PJP (or an inbound event it sent) is the root
	// cause -- a timeout/5xx talking to the provider, or a webhook event
	// whose timing/dedup this template correctly rejected.
	SourceProvider Source = "provider"
)

// Error is the concrete error type carrying a Code plus a human-readable
// message and, optionally, a wrapped cause.
type Error struct {
	Code    Code
	Message string
	Cause   error
}

func (e *Error) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *Error) Unwrap() error {
	return e.Cause
}

func New(code Code, message string) *Error {
	return &Error{Code: code, Message: message}
}

// Wrap builds an *Error that wraps an underlying cause, preserving it for
// errors.Is/errors.As/errors.Unwrap chains while still exposing a stable
// Code for transport-layer mapping.
func Wrap(code Code, message string, cause error) *Error {
	return &Error{Code: code, Message: message, Cause: cause}
}

// CodeOf extracts the Code from err if it is (or wraps) an *Error, and
// CodeInternal otherwise. Transports use this to decide their response
// without needing a type switch at every call site.
func CodeOf(err error) Code {
	var appErr *Error
	if errors.As(err, &appErr) {
		return appErr.Code
	}
	return CodeInternal
}

// sourceByCode is the fixed Code -> Source mapping SourceOf reads. Codes not
// listed default to SourceInternal (see SourceOf) -- the safe default for
// "something this template doesn't have a specific taxonomy entry for yet",
// consistent with CodeOf's own CodeInternal fallback for a plain (non
// apperror) wrapped error, e.g. a raw pgx/DB failure that was never given
// its own Code.
var sourceByCode = map[Code]Source{
	CodeValidation:            SourceClient,
	CodeIdempotencyConflict:   SourceClient,
	CodeIdempotencyInProgress: SourceClient,
	CodeNotFound:              SourceClient,
	CodeRefundLimitExceeded:   SourceClient,
	CodeUnauthorized:          SourceClient,
	CodeInvalidTransition:     SourceClient,
	CodeTerminalState:         SourceClient,

	CodeProviderTimeout: SourceProvider,
	CodeProviderError:   SourceProvider,
	// Dedup/staleness rejections are driven by provider-event timing
	// (webhook redelivery, out-of-order delivery), not a caller mistake or
	// a bug in this codebase.
	CodeDuplicateEvent: SourceProvider,
	CodeStaleEvent:     SourceProvider,

	// Both of these are OUR configuration/registration state, not the
	// PJP's fault -- an unregistered provider name or a dynamic-config
	// provider-enabled=false is a decision made in this deployment.
	CodeProviderNotRegistered: SourceInternal,
	CodeProviderDisabled:      SourceInternal,
	CodeInternal:              SourceInternal,
}

// SourceOf reports code's Source per sourceByCode, defaulting to
// SourceInternal for any Code not explicitly listed there (including a
// plain wrapped error CodeOf already resolved to CodeInternal). Call sites
// logging an error should include this as a structured attribute (e.g.
// `"source", apperror.SourceOf(apperror.CodeOf(err))`) alongside a
// `"layer"` attribute identifying which architectural layer logged it, so
// an incident reader can tell "our bug vs. bad input vs. the PJP" without
// reading code.
func SourceOf(code Code) Source {
	if src, ok := sourceByCode[code]; ok {
		return src
	}
	return SourceInternal
}
