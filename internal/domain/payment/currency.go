package payment

import (
	"Fisher-Mapper/internal/domain/apperror"
)

// iso4217Codes is an allowlist of active ISO 4217 three-letter currency
// codes. Deliberately EXCLUDES the special-purpose X-codes (XXX "no
// currency", XTS "reserved for testing", XDR "IMF Special Drawing Rights",
// and the precious-metal codes XAU/XAG/XPT/XPD) -- those are real ISO 4217
// codes, but none of them denote a currency a payment/refund/payout can
// actually be settled in, so accepting them here would be a real gap, not a
// completeness win.
//
// This is a template default covering the currencies a PJP integration is
// realistically going to need; an integrator adding a PJP that settles in a
// currency missing here should extend this map, not bypass ValidateCurrency.
var iso4217Codes = map[string]struct{}{
	"AED": {}, "AFN": {}, "ALL": {}, "AMD": {}, "ANG": {}, "AOA": {}, "ARS": {}, "AUD": {},
	"AWG": {}, "AZN": {}, "BAM": {}, "BBD": {}, "BDT": {}, "BGN": {}, "BHD": {}, "BIF": {},
	"BMD": {}, "BND": {}, "BOB": {}, "BRL": {}, "BSD": {}, "BTN": {}, "BWP": {}, "BYN": {},
	"BZD": {}, "CAD": {}, "CDF": {}, "CHF": {}, "CLP": {}, "CNY": {}, "COP": {}, "CRC": {},
	"CUP": {}, "CVE": {}, "CZK": {}, "DJF": {}, "DKK": {}, "DOP": {}, "DZD": {}, "EGP": {},
	"ERN": {}, "ETB": {}, "EUR": {}, "FJD": {}, "FKP": {}, "GBP": {}, "GEL": {}, "GHS": {},
	"GIP": {}, "GMD": {}, "GNF": {}, "GTQ": {}, "GYD": {}, "HKD": {}, "HNL": {}, "HTG": {},
	"HUF": {}, "IDR": {}, "ILS": {}, "INR": {}, "IQD": {}, "IRR": {}, "ISK": {}, "JMD": {},
	"JOD": {}, "JPY": {}, "KES": {}, "KGS": {}, "KHR": {}, "KMF": {}, "KPW": {}, "KRW": {},
	"KWD": {}, "KYD": {}, "KZT": {}, "LAK": {}, "LBP": {}, "LKR": {}, "LRD": {}, "LSL": {},
	"LYD": {}, "MAD": {}, "MDL": {}, "MGA": {}, "MKD": {}, "MMK": {}, "MNT": {}, "MOP": {},
	"MRU": {}, "MUR": {}, "MVR": {}, "MWK": {}, "MXN": {}, "MYR": {}, "MZN": {}, "NAD": {},
	"NGN": {}, "NIO": {}, "NOK": {}, "NPR": {}, "NZD": {}, "OMR": {}, "PAB": {}, "PEN": {},
	"PGK": {}, "PHP": {}, "PKR": {}, "PLN": {}, "PYG": {}, "QAR": {}, "RON": {}, "RSD": {},
	"RUB": {}, "RWF": {}, "SAR": {}, "SBD": {}, "SCR": {}, "SDG": {}, "SEK": {}, "SGD": {},
	"SHP": {}, "SLE": {}, "SOS": {}, "SRD": {}, "SSP": {}, "STN": {}, "SYP": {}, "SZL": {},
	"THB": {}, "TJS": {}, "TMT": {}, "TND": {}, "TOP": {}, "TRY": {}, "TTD": {}, "TWD": {},
	"TZS": {}, "UAH": {}, "UGX": {}, "USD": {}, "UYU": {}, "UZS": {}, "VES": {}, "VND": {},
	"VUV": {}, "WST": {}, "XAF": {}, "XCD": {}, "XOF": {}, "XPF": {}, "YER": {}, "ZAR": {},
	"ZMW": {}, "ZWL": {},
}

// ValidateCurrency enforces ISO 4217 at the domain/service boundary: a
// financial record's `currency` column must never accept arbitrary garbage.
// Rejects anything that is not exactly 3 uppercase ASCII letters (format
// check) AND not a recognized, currently-settleable ISO 4217 code
// (allowlist check) -- e.g. "usd" (wrong case), "US" (wrong length), and
// "XXX" ("no currency", a real ISO 4217 code but not a currency) are all
// rejected.
func ValidateCurrency(code string) error {
	if len(code) != 3 {
		return apperror.New(apperror.CodeValidation, "currency must be a 3-letter ISO 4217 code")
	}
	for _, r := range code {
		if r < 'A' || r > 'Z' {
			return apperror.New(apperror.CodeValidation, "currency must be 3 uppercase ASCII letters (ISO 4217)")
		}
	}
	if _, ok := iso4217Codes[code]; !ok {
		return apperror.New(apperror.CodeValidation, "currency is not a recognized ISO 4217 code: "+code)
	}
	return nil
}
