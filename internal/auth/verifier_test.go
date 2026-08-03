package auth

import (
	"context"
	"errors"
	"testing"
	"time"

	"Fisher-Mapper/internal/apperror"
)

// fixedClock returns a fixed time.Time, so the ±window test below is
// deterministic instead of racing against the wall clock / sleeping.
func fixedClock(t time.Time) func() time.Time {
	return func() time.Time { return t }
}

func TestHMACVerifier_ValidSignatureWithinWindow(t *testing.T) {
	secret := "shhh"
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	ts := now.Add(-1 * time.Minute) // inside a 5-minute window

	body := []byte(`{"provider_event_id":"evt_1"}`)
	signer := NewHMACSigner(secret)
	headers, err := signer.Sign(SignInput{Method: "POST", Path: "/webhooks/mock", Body: body, Timestamp: ts})
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	v := &HMACVerifier{Secret: secret, Window: 5 * time.Minute, Clock: fixedClock(now)}
	err = v.Verify(context.Background(), VerifyInput{
		Method:    "POST",
		Path:      "/webhooks/mock",
		Body:      body,
		Timestamp: ts,
		Signature: headers[signer.SignatureHeader],
	})
	if err != nil {
		t.Fatalf("Verify() = %v, want nil", err)
	}
}

// TestHMACVerifier_TimestampOutsideWindowRejected is Phase 2 validation 7:
// a webhook timestamped 10 minutes in the past against a ±5-minute window
// must be rejected by the Verifier — signature validity does not save it.
func TestHMACVerifier_TimestampOutsideWindowRejected(t *testing.T) {
	secret := "shhh"
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	ts := now.Add(-10 * time.Minute) // outside a 5-minute window

	body := []byte(`{"provider_event_id":"evt_2"}`)
	signer := NewHMACSigner(secret)
	headers, err := signer.Sign(SignInput{Method: "POST", Path: "/webhooks/mock", Body: body, Timestamp: ts})
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	v := &HMACVerifier{Secret: secret, Window: 5 * time.Minute, Clock: fixedClock(now)}
	err = v.Verify(context.Background(), VerifyInput{
		Method:    "POST",
		Path:      "/webhooks/mock",
		Body:      body,
		Timestamp: ts,
		Signature: headers[signer.SignatureHeader],
	})
	if err == nil {
		t.Fatal("Verify() = nil, want a rejection for a timestamp outside the skew window")
	}
	if apperror.CodeOf(err) != apperror.CodeUnauthorized {
		t.Errorf("CodeOf(err) = %v, want %v", apperror.CodeOf(err), apperror.CodeUnauthorized)
	}
}

// TestHMACVerifier_FutureTimestampOutsideWindowRejected: skew is checked as
// an absolute value, so a timestamp in the future outside the window is
// rejected too, not just one in the past.
func TestHMACVerifier_FutureTimestampOutsideWindowRejected(t *testing.T) {
	secret := "shhh"
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	ts := now.Add(10 * time.Minute)

	body := []byte(`{}`)
	signer := NewHMACSigner(secret)
	headers, err := signer.Sign(SignInput{Method: "POST", Path: "/webhooks/mock", Body: body, Timestamp: ts})
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	v := &HMACVerifier{Secret: secret, Window: 5 * time.Minute, Clock: fixedClock(now)}
	err = v.Verify(context.Background(), VerifyInput{
		Method: "POST", Path: "/webhooks/mock", Body: body, Timestamp: ts,
		Signature: headers[signer.SignatureHeader],
	})
	if apperror.CodeOf(err) != apperror.CodeUnauthorized {
		t.Errorf("CodeOf(err) = %v, want %v", apperror.CodeOf(err), apperror.CodeUnauthorized)
	}
}

func TestHMACVerifier_TamperedBodyRejected(t *testing.T) {
	secret := "shhh"
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	signer := NewHMACSigner(secret)
	headers, err := signer.Sign(SignInput{Method: "POST", Path: "/webhooks/mock", Body: []byte(`{"amount":100}`), Timestamp: now})
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	v := &HMACVerifier{Secret: secret, Window: 5 * time.Minute, Clock: fixedClock(now)}
	err = v.Verify(context.Background(), VerifyInput{
		Method:    "POST",
		Path:      "/webhooks/mock",
		Body:      []byte(`{"amount":100000}`), // tampered after signing
		Timestamp: now,
		Signature: headers[signer.SignatureHeader],
	})
	if apperror.CodeOf(err) != apperror.CodeUnauthorized {
		t.Errorf("CodeOf(err) = %v, want %v (tampered body must not verify)", apperror.CodeOf(err), apperror.CodeUnauthorized)
	}
}

func TestHMACVerifier_MissingTimestampRejected(t *testing.T) {
	v := &HMACVerifier{Secret: "shhh", Window: 5 * time.Minute}
	err := v.Verify(context.Background(), VerifyInput{Method: "POST", Path: "/x", Body: []byte(`{}`), Signature: "deadbeef"})
	if apperror.CodeOf(err) != apperror.CodeUnauthorized {
		t.Errorf("CodeOf(err) = %v, want %v", apperror.CodeOf(err), apperror.CodeUnauthorized)
	}
}

// fakeDedup lets the dedup-before-apply behavior be tested without a DB.
type fakeDedup struct {
	seen map[string]bool
	err  error
}

func (f fakeDedup) Seen(_ context.Context, provider, eventID string) (bool, error) {
	if f.err != nil {
		return false, f.err
	}
	return f.seen[provider+"/"+eventID], nil
}

func TestHMACVerifier_DedupRejectsAlreadySeenEvent(t *testing.T) {
	secret := "shhh"
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	body := []byte(`{}`)

	signer := NewHMACSigner(secret)
	headers, err := signer.Sign(SignInput{Method: "POST", Path: "/webhooks/mock", Body: body, Timestamp: now})
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	v := &HMACVerifier{
		Secret: secret,
		Window: 5 * time.Minute,
		Clock:  fixedClock(now),
		Dedup:  fakeDedup{seen: map[string]bool{"mock/evt_dup": true}},
	}

	err = v.Verify(context.Background(), VerifyInput{
		Method: "POST", Path: "/webhooks/mock", Body: body, Timestamp: now,
		Signature: headers[signer.SignatureHeader],
		Provider:  "mock", ProviderEventID: "evt_dup",
	})
	if apperror.CodeOf(err) != apperror.CodeDuplicateEvent {
		t.Fatalf("CodeOf(err) = %v, want %v", apperror.CodeOf(err), apperror.CodeDuplicateEvent)
	}

	// A fresh event id, otherwise identical, must pass.
	err = v.Verify(context.Background(), VerifyInput{
		Method: "POST", Path: "/webhooks/mock", Body: body, Timestamp: now,
		Signature: headers[signer.SignatureHeader],
		Provider:  "mock", ProviderEventID: "evt_new",
	})
	if err != nil {
		t.Fatalf("Verify() with an unseen event id = %v, want nil", err)
	}
}

func TestHMACVerifier_DedupCheckErrorPropagates(t *testing.T) {
	secret := "shhh"
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	body := []byte(`{}`)

	signer := NewHMACSigner(secret)
	headers, _ := signer.Sign(SignInput{Method: "POST", Path: "/x", Body: body, Timestamp: now})

	boom := errors.New("db unreachable")
	v := &HMACVerifier{Secret: secret, Window: 5 * time.Minute, Clock: fixedClock(now), Dedup: fakeDedup{err: boom}}

	err := v.Verify(context.Background(), VerifyInput{
		Method: "POST", Path: "/x", Body: body, Timestamp: now,
		Signature: headers[signer.SignatureHeader], Provider: "mock", ProviderEventID: "evt",
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Verify() error = %v, want it to wrap %v", err, boom)
	}
}
