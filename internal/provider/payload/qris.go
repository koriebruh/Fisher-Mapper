package payload

import "time"

// QRIS is the payload for PaymentMethod "qris" -- QRString is the string a
// client renders as a scannable QR code (opaque provider-issued data, never
// derived from card/bank data), nil when not applicable to this charge.
type QRIS struct {
	QRString  *string    `json:"qr_string"`
	ExpiresAt *time.Time `json:"expires_at"`
}
