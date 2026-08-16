package payment

import (
	"net/url"

	"Fisher-Mapper/internal/domain/apperror"
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
func ValidateCallbackURL(raw string) error {
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
	return nil
}
