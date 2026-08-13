package payment

import (
	"time"

	"Fisher-Mapper/internal/domain/apperror"
)

// Status is the payment's explicit state machine value. Locked in by plan
// "Invarian Uang — Decide Now" item 7: pending -> processing ->
// succeeded|failed, terminal states immutable.
type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusSucceeded  Status = "succeeded"
	StatusFailed     Status = "failed"
)

// IsTerminal reports whether s is a terminal state (succeeded/failed).
// Terminal states never transition again — enforced by Transition below.
func IsTerminal(s Status) bool {
	return s == StatusSucceeded || s == StatusFailed
}

// allowedTransitions is the only graph of moves Transition will accept.
// Deliberately narrow (no pending -> succeeded shortcut) so every payment's
// history always passes through "processing" — the point at which a
// provider call has actually been attempted.
var allowedTransitions = map[Status]map[Status]bool{
	StatusPending:    {StatusProcessing: true},
	StatusProcessing: {StatusSucceeded: true, StatusFailed: true},
}

// Transition validates a proposed move from cur to next.
//
// eventTS is the timestamp of the event driving this transition (e.g. "now"
// for an internally-driven transition, or the provider's event timestamp
// for a webhook-driven one). lastEventTS is the timestamp of the last event
// already applied to this payment (payments.last_event_at).
//
// Rules, in order:
//  1. A terminal current state can never transition again (immutable) —
//     checked first and unconditionally, regardless of timestamps: no event,
//     however new, revives a terminal payment via this path.
//  2. An event older than the last applied event is stale and rejected —
//     state must never move backwards in time.
//  3. The move itself must be in allowedTransitions.
//
// Pure function, no I/O — the repository is responsible for reading
// current state/lastEventTS under `SELECT ... FOR UPDATE` and calling this
// inside that same transaction before writing anything.
func Transition(cur, next Status, eventTS, lastEventTS time.Time) error {
	if IsTerminal(cur) {
		return apperror.New(apperror.CodeTerminalState, "payment: current state is terminal, cannot transition")
	}

	if !eventTS.IsZero() && !lastEventTS.IsZero() && eventTS.Before(lastEventTS) {
		return apperror.New(apperror.CodeStaleEvent, "payment: event timestamp is older than last applied event")
	}

	if !allowedTransitions[cur][next] {
		return apperror.New(apperror.CodeInvalidTransition, "payment: transition "+string(cur)+" -> "+string(next)+" is not allowed")
	}

	return nil
}
