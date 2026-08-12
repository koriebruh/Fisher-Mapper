// Package auth separates outbound request signing (Signer, used when we
// call a PJP) from inbound webhook verification (Verifier, used when a PJP
// calls us) — per the plan, these are kept as distinct interfaces
// deliberately, so the two directions never get swapped by accident.
//
// Phase 2 ships two Signer implementations (static API key, HMAC-SHA256);
// OAuth2 client_credentials, session login, and mTLS are named in the plan
// as later additions behind the same interface.
package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

// SignInput is what a Signer needs to produce outbound auth headers.
type SignInput struct {
	Method    string
	Path      string
	Body      []byte
	Timestamp time.Time
}

// Signer produces the headers to attach to an outbound request to a PJP.
type Signer interface {
	Sign(in SignInput) (map[string]string, error)
}

// APIKeySigner attaches a static API key header. This is the simplest auth
// mode PJPs offer — no request-specific computation.
type APIKeySigner struct {
	HeaderName string
	APIKey     string
}

// NewAPIKeySigner builds an APIKeySigner. headerName defaults to
// "X-Api-Key" if empty.
func NewAPIKeySigner(headerName, apiKey string) APIKeySigner {
	if headerName == "" {
		headerName = "X-Api-Key"
	}
	return APIKeySigner{HeaderName: headerName, APIKey: apiKey}
}

func (s APIKeySigner) Sign(SignInput) (map[string]string, error) {
	return map[string]string{s.HeaderName: s.APIKey}, nil
}

// HMACSigner signs a canonical string (timestamp + method + path + body)
// with HMAC-SHA256 and attaches both the signature and the timestamp used
// to compute it — the timestamp MUST travel with the signature so the
// receiving Verifier can check the signature was computed over the same
// timestamp it validates the skew window against (see verifier.go).
type HMACSigner struct {
	Secret          string
	SignatureHeader string
	TimestampHeader string
}

func NewHMACSigner(secret string) HMACSigner {
	return HMACSigner{
		Secret:          secret,
		SignatureHeader: "X-Signature",
		TimestampHeader: "X-Timestamp",
	}
}

// CanonicalString builds the exact string that gets HMAC-signed. Exported
// so a Verifier implementation (or a test) can recompute it independently
// without duplicating the format by hand.
func CanonicalString(method, path string, timestamp time.Time, body []byte) string {
	return fmt.Sprintf("%d\n%s\n%s\n%s", timestamp.Unix(), method, path, body)
}

func (s HMACSigner) Sign(in SignInput) (map[string]string, error) {
	ts := in.Timestamp
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	canonical := CanonicalString(in.Method, in.Path, ts, in.Body)

	mac := hmac.New(sha256.New, []byte(s.Secret))
	mac.Write([]byte(canonical))
	sig := hex.EncodeToString(mac.Sum(nil))

	return map[string]string{
		s.SignatureHeader: sig,
		s.TimestampHeader: fmt.Sprintf("%d", ts.Unix()),
	}, nil
}

var (
	_ Signer = APIKeySigner{}
	_ Signer = HMACSigner{}
)
