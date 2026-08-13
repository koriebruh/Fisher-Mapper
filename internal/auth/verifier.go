package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"Fisher-Mapper/internal/domain/apperror"
)

// DedupChecker reports whether a (provider, providerEventID) pair has
// already been applied. Verify calls this — dedup is checked as part of
// webhook verification, before the caller ever reaches state-transition
// logic, per the plan's explicit requirement that this is not optional.
//
// The DB unique index on payment_events(provider, provider_event_id)
// (see migration 00002) is the authoritative, atomic backstop for this —
// DedupChecker is a fast-path rejection so an already-seen event never even
// reaches the state machine, but a race between the check and the DB
// insert is still resolved correctly by the unique constraint.
type DedupChecker interface {
	Seen(ctx context.Context, provider, providerEventID string) (bool, error)
}

// VerifyInput is what a Verifier needs to validate an inbound webhook call.
type VerifyInput struct {
	Method          string
	Path            string
	Body            []byte
	Timestamp       time.Time
	Signature       string
	Provider        string
	ProviderEventID string
}

// Verifier validates an inbound webhook request: signature, timestamp
// window (clock-skew tolerance), and event dedup. All three checks happen
// before the caller is told the event is safe to apply.
type Verifier interface {
	Verify(ctx context.Context, in VerifyInput) error
}

// HMACVerifier is the HMAC-SHA256 Verifier implementation, symmetric with
// HMACSigner: it recomputes CanonicalString(method, path, timestamp, body)
// and compares against the provided signature with hmac.Equal (constant
// time, so signature comparison is not a timing side channel).
type HMACVerifier struct {
	Secret string

	// Window is the maximum allowed absolute difference between the
	// event's timestamp and "now" (clock skew tolerance). Per plan: ± a
	// few minutes, e.g. 5 minutes.
	Window time.Duration

	// Clock overrides time.Now for deterministic tests. Defaults to
	// time.Now when nil (see now()).
	Clock func() time.Time

	// Dedup, if non-nil, is consulted after signature+timestamp checks
	// pass. May be left nil in contexts (e.g. pure unit tests of
	// signature/timestamp behavior) where dedup is verified separately.
	Dedup DedupChecker
}

func (v *HMACVerifier) now() time.Time {
	if v.Clock != nil {
		return v.Clock()
	}
	return time.Now().UTC()
}

func (v *HMACVerifier) Verify(ctx context.Context, in VerifyInput) error {
	if in.Timestamp.IsZero() {
		return apperror.New(apperror.CodeUnauthorized, "webhook: missing timestamp")
	}

	skew := v.now().Sub(in.Timestamp)
	if skew < 0 {
		skew = -skew
	}
	if skew > v.Window {
		return apperror.New(apperror.CodeUnauthorized, "webhook: timestamp outside allowed skew window")
	}

	canonical := CanonicalString(in.Method, in.Path, in.Timestamp, in.Body)
	mac := hmac.New(sha256.New, []byte(v.Secret))
	mac.Write([]byte(canonical))
	expected := mac.Sum(nil)

	got, err := hex.DecodeString(in.Signature)
	if err != nil || !hmac.Equal(got, expected) {
		return apperror.New(apperror.CodeUnauthorized, "webhook: signature mismatch")
	}

	if v.Dedup != nil {
		seen, err := v.Dedup.Seen(ctx, in.Provider, in.ProviderEventID)
		if err != nil {
			return apperror.Wrap(apperror.CodeInternal, "webhook: dedup check failed", err)
		}
		if seen {
			return apperror.New(apperror.CodeDuplicateEvent, "webhook: provider_event_id already applied")
		}
	}

	return nil
}

var _ Verifier = (*HMACVerifier)(nil)
