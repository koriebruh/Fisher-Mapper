package payment

import (
	"context"
	"testing"

	"Fisher-Mapper/internal/domain/apperror"
)

func TestValidateCallbackURL(t *testing.T) {
	cases := []struct {
		name    string
		url     string
		wantErr bool
	}{
		// 203.0.113.0/24 (TEST-NET-3) is reserved for documentation, not
		// private/loopback/link-local -- Go's net.IP treats it as public, so
		// this exercises the "accepted" path without a real DNS lookup or
		// a real reachable host.
		{"valid https", "https://203.0.113.5/webhook", false},
		{"valid http", "http://203.0.113.5/webhook", false},
		{"valid with port and path", "https://203.0.113.5:8443/hooks/payments", false},
		// url.Parse alone accepts this with NO error (parses as a relative
		// path) -- the whole reason Scheme/Host must be checked explicitly,
		// not just that Parse succeeded.
		{"not a url rejected", "not-a-url", true},
		{"missing scheme rejected", "example.com/webhook", true},
		{"ftp scheme rejected", "ftp://example.com/file", true},
		{"scheme with no host rejected", "https://", true},
		{"empty rejected", "", true},
		// SSRF: IP-literal cases take ssrf.CheckHost's fast path (no DNS,
		// deterministic in any test environment, network or not).
		{"loopback IPv4 rejected", "http://127.0.0.1/webhook", true},
		{"loopback IPv6 rejected", "http://[::1]/webhook", true},
		// The cloud metadata endpoint -- the concrete SSRF target this
		// check exists to close off.
		{"cloud metadata endpoint rejected", "http://169.254.169.254/latest/meta-data/", true},
		{"private 10/8 rejected", "http://10.0.0.5/webhook", true},
		{"private 192.168/16 rejected", "http://192.168.1.1/webhook", true},
		{"unspecified rejected", "http://0.0.0.0/webhook", true},
		// "localhost" resolves via the local hosts database, not a real DNS
		// round-trip -- deterministic without network access.
		{"localhost hostname rejected", "http://localhost/webhook", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateCallbackURL(context.Background(), tc.url)
			if tc.wantErr && err == nil {
				t.Fatalf("ValidateCallbackURL(%q) = nil, want error", tc.url)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("ValidateCallbackURL(%q) = %v, want nil", tc.url, err)
			}
			if tc.wantErr && apperror.CodeOf(err) != apperror.CodeValidation {
				t.Errorf("CodeOf(err) = %v, want %v", apperror.CodeOf(err), apperror.CodeValidation)
			}
		})
	}
}
