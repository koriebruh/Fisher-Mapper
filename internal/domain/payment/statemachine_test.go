package payment

import (
	"errors"
	"testing"
	"time"

	"Fisher-Mapper/internal/domain/apperror"
)

func TestTransition_ValidMoves(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Minute)

	cases := []struct {
		name string
		cur  Status
		next Status
	}{
		{"pending to processing", StatusPending, StatusProcessing},
		{"processing to succeeded", StatusProcessing, StatusSucceeded},
		{"processing to failed", StatusProcessing, StatusFailed},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := Transition(tc.cur, tc.next, t1, t0); err != nil {
				t.Errorf("Transition(%s, %s) = %v, want nil", tc.cur, tc.next, err)
			}
		})
	}
}

func TestTransition_InvalidMovesRejected(t *testing.T) {
	now := time.Now()

	cases := []struct {
		name string
		cur  Status
		next Status
	}{
		{"pending cannot skip straight to succeeded", StatusPending, StatusSucceeded},
		{"pending cannot skip straight to failed", StatusPending, StatusFailed},
		{"processing cannot go back to pending", StatusProcessing, StatusPending},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Transition(tc.cur, tc.next, now, now)
			if err == nil {
				t.Fatalf("Transition(%s, %s) = nil, want an error", tc.cur, tc.next)
			}
			if apperror.CodeOf(err) != apperror.CodeInvalidTransition {
				t.Errorf("CodeOf(err) = %v, want %v", apperror.CodeOf(err), apperror.CodeInvalidTransition)
			}
		})
	}
}

// TestTransition_TerminalStateIsImmutable covers plan validation 5's literal
// wording: once succeeded, an older-timestamped event trying to move to
// failed must be rejected and the state must not move. Terminal-immutability
// alone is enough to reject this case (see the next test for the
// stale-event guard exercised independently of terminality).
func TestTransition_TerminalStateIsImmutable(t *testing.T) {
	succeededAt := time.Now()
	olderEventTS := succeededAt.Add(-10 * time.Minute)

	err := Transition(StatusSucceeded, StatusFailed, olderEventTS, succeededAt)
	if err == nil {
		t.Fatal("Transition from a terminal state = nil, want an error")
	}
	if apperror.CodeOf(err) != apperror.CodeTerminalState {
		t.Errorf("CodeOf(err) = %v, want %v", apperror.CodeOf(err), apperror.CodeTerminalState)
	}

	// A newer timestamp does not help either — terminal is terminal.
	newerEventTS := succeededAt.Add(10 * time.Minute)
	err = Transition(StatusSucceeded, StatusFailed, newerEventTS, succeededAt)
	if apperror.CodeOf(err) != apperror.CodeTerminalState {
		t.Errorf("CodeOf(err) = %v, want %v (newer timestamp must not revive a terminal state)", apperror.CodeOf(err), apperror.CodeTerminalState)
	}
}

// TestTransition_StaleEventRejectedOnNonTerminalState exercises the
// timestamp-ordering guard on its own, independent of terminal-state
// immutability: without this test, TestTransition_TerminalStateIsImmutable
// alone could pass even if the stale-timestamp check were deleted entirely
// (since a terminal current state is already rejected unconditionally).
func TestTransition_StaleEventRejectedOnNonTerminalState(t *testing.T) {
	lastApplied := time.Date(2026, 1, 1, 12, 10, 0, 0, time.UTC) // T10
	staleEvent := time.Date(2026, 1, 1, 12, 5, 0, 0, time.UTC)   // T5, older
	freshEvent := time.Date(2026, 1, 1, 12, 15, 0, 0, time.UTC)  // T15, newer

	err := Transition(StatusProcessing, StatusSucceeded, staleEvent, lastApplied)
	if err == nil {
		t.Fatal("Transition with a stale event timestamp = nil, want an error")
	}
	if apperror.CodeOf(err) != apperror.CodeStaleEvent {
		t.Errorf("CodeOf(err) = %v, want %v", apperror.CodeOf(err), apperror.CodeStaleEvent)
	}

	if err := Transition(StatusProcessing, StatusSucceeded, freshEvent, lastApplied); err != nil {
		t.Errorf("Transition with a newer event timestamp = %v, want nil", err)
	}
}

func TestIsTerminal(t *testing.T) {
	terminal := []Status{StatusSucceeded, StatusFailed}
	nonTerminal := []Status{StatusPending, StatusProcessing}

	for _, s := range terminal {
		if !IsTerminal(s) {
			t.Errorf("IsTerminal(%s) = false, want true", s)
		}
	}
	for _, s := range nonTerminal {
		if IsTerminal(s) {
			t.Errorf("IsTerminal(%s) = true, want false", s)
		}
	}
}

// sanity: errors returned are usable with errors.Is/As via apperror.Error.
func TestTransition_ErrorIsApperrorError(t *testing.T) {
	err := Transition(StatusSucceeded, StatusFailed, time.Now(), time.Now())
	var appErr *apperror.Error
	if !errors.As(err, &appErr) {
		t.Fatalf("error %v is not an *apperror.Error", err)
	}
}
