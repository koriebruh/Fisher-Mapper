package apperror

import (
	"errors"
	"fmt"
	"log/slog"
	"testing"
)

// TestLogAttr_ClassifiesEveryDefinedCode proves LogAttr (the log-call-site
// helper) agrees with SourceOf for every Code currently in the taxonomy --
// the whole point of the helper is that a call site never has to re-derive
// this by hand, so it must never silently diverge from SourceOf itself.
func TestLogAttr_ClassifiesEveryDefinedCode(t *testing.T) {
	cases := []struct {
		code Code
		want Source
	}{
		{CodeValidation, SourceClient},
		{CodeIdempotencyConflict, SourceClient},
		{CodeIdempotencyInProgress, SourceClient},
		{CodeNotFound, SourceClient},
		{CodeRefundLimitExceeded, SourceClient},
		{CodeUnauthorized, SourceClient},
		{CodeInvalidTransition, SourceClient},
		{CodeTerminalState, SourceClient},
		{CodeProviderTimeout, SourceProvider},
		{CodeProviderError, SourceProvider},
		{CodeDuplicateEvent, SourceProvider},
		{CodeStaleEvent, SourceProvider},
		{CodeProviderNotRegistered, SourceInternal},
		{CodeProviderDisabled, SourceInternal},
		{CodeInternal, SourceInternal},
	}

	for _, tc := range cases {
		t.Run(string(tc.code), func(t *testing.T) {
			err := New(tc.code, "boom")
			got := LogAttr(err)
			want := slog.String("source", string(tc.want))
			if got.Key != want.Key || got.Value.String() != want.Value.String() {
				t.Errorf("LogAttr(%v) = %v, want %v", tc.code, got, want)
			}
			if SourceOf(tc.code) != tc.want {
				t.Errorf("SourceOf(%v) = %v, want %v (test table itself out of sync)", tc.code, SourceOf(tc.code), tc.want)
			}
		})
	}
}

// TestLogAttr_PlainErrorDefaultsToInternal: a raw (non-apperror) error --
// e.g. an unwrapped DB/infra failure this template never gave its own Code
// -- must classify as SourceInternal, the same safe default CodeOf/SourceOf
// already document.
func TestLogAttr_PlainErrorDefaultsToInternal(t *testing.T) {
	got := LogAttr(errors.New("raw failure"))
	want := slog.String("source", string(SourceInternal))
	if got.Value.String() != want.Value.String() {
		t.Errorf("LogAttr(plain error) = %v, want %v", got, want)
	}
}

// TestLogAttr_TraversesWrappedChain: a plain error wrapped via fmt.Errorf's
// %w around an *Error must still classify by the wrapped Error's Code --
// this is what lets a caller several layers away from where the Error was
// constructed (e.g. reconciliation/job.go wrapping ReconcilePayment's
// return) still log the correct source without re-deriving it.
func TestLogAttr_TraversesWrappedChain(t *testing.T) {
	inner := New(CodeStaleEvent, "too old")
	wrapped := fmt.Errorf("reconcile payment: apply transition: %w", inner)

	got := LogAttr(wrapped)
	want := slog.String("source", string(SourceProvider))
	if got.Value.String() != want.Value.String() {
		t.Errorf("LogAttr(wrapped) = %v, want %v", got, want)
	}
}
