package payment

import (
	"testing"

	"Fisher-Mapper/internal/domain/apperror"
)

func TestValidateCallbackURL(t *testing.T) {
	cases := []struct {
		name    string
		url     string
		wantErr bool
	}{
		{"valid https", "https://example.com/webhook", false},
		{"valid http", "http://example.com/webhook", false},
		{"valid with port and path", "https://example.com:8443/hooks/payments", false},
		// url.Parse alone accepts this with NO error (parses as a relative
		// path) -- the whole reason Scheme/Host must be checked explicitly,
		// not just that Parse succeeded.
		{"not a url rejected", "not-a-url", true},
		{"missing scheme rejected", "example.com/webhook", true},
		{"ftp scheme rejected", "ftp://example.com/file", true},
		{"scheme with no host rejected", "https://", true},
		{"empty rejected", "", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateCallbackURL(tc.url)
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
