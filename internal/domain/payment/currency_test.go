package payment

import (
	"testing"

	"Fisher-Mapper/internal/domain/apperror"
)

func TestValidateCurrency(t *testing.T) {
	cases := []struct {
		name    string
		code    string
		wantErr bool
	}{
		{"valid USD", "USD", false},
		{"valid IDR", "IDR", false},
		{"valid EUR", "EUR", false},
		{"lowercase rejected", "usd", true},
		{"mixed case rejected", "Usd", true},
		{"two letters rejected", "US", true},
		{"four letters rejected", "USDT", true},
		{"empty rejected", "", true},
		{"digits rejected", "12A", true},
		// XXX is a real ISO 4217 code ("no currency") but not a settleable
		// currency -- must be rejected, not silently accepted by a naive
		// format-only check.
		{"XXX rejected", "XXX", true},
		{"XTS rejected", "XTS", true},
		{"unrecognized code rejected", "ZZZ", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateCurrency(tc.code)
			if tc.wantErr && err == nil {
				t.Fatalf("ValidateCurrency(%q) = nil, want error", tc.code)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("ValidateCurrency(%q) = %v, want nil", tc.code, err)
			}
			if tc.wantErr && apperror.CodeOf(err) != apperror.CodeValidation {
				t.Errorf("CodeOf(err) = %v, want %v", apperror.CodeOf(err), apperror.CodeValidation)
			}
		})
	}
}
