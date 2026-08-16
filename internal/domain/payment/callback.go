package payment

import (
	"context"
	"net/url"

	"Fisher-Mapper/internal/domain/apperror"
	"Fisher-Mapper/internal/resilience/ssrf"
)

// ValidateCallbackURL enforces a minimal, non-over-engineered format check
// on a caller-supplied callback URL, mirroring ValidateCurrency's role at
// the domain/service boundary: reject anything that isn't parseable as an
// absolute http(s) URL, before it's ever persisted or dialed (Service.
// WithCallbackNotifier). Reuses CodeValidation (not a new Code) -- this is
// exactly the same category of caller-input problem ValidateCurrency
// already reports, and reusing it means no new entry is needed in
// apperror's Source map or either transport's status-code map.
//
// net/url.Parse alone is not enough: it happily parses "not-a-url" as a
// relative path with no error, so both Scheme and Host must be checked
// explicitly.
//
// SSRF defense in depth: also rejects a host that is (or currently resolves
// to) a loopback/link-local/private/unspecified address -- see
// ssrf.CheckHost's doc. This is advisory, not the authoritative guard
// (a hostname's DNS answer can change between now and actual delivery,
// i.e. DNS rebinding) -- the authoritative check is ssrf.SafeDialer, wired
// into the concrete CallbackNotifier's http.Client at dial time.
func ValidateCallbackURL(ctx context.Context, raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return apperror.New(apperror.CodeValidation, "callback_url is not a valid URL")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return apperror.New(apperror.CodeValidation, "callback_url must use http or https")
	}
	if u.Host == "" {
		return apperror.New(apperror.CodeValidation, "callback_url must be an absolute URL with a host")
	}
	if err := ssrf.CheckHost(ctx, u.Hostname()); err != nil {
		return apperror.New(apperror.CodeValidation, "callback_url must not point at a private/internal address")
	}
	return nil
}
