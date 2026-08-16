package payload

import "time"

// EWallet is the payload for PaymentMethod "ewallet" -- the customer
// completes payment via a redirect/deeplink to their e-wallet app.
type EWallet struct {
	RedirectURL *string    `json:"redirect_url"`
	ExpiresAt   *time.Time `json:"expires_at"`
}
