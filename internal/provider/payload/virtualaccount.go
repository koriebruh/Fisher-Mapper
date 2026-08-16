package payload

import "time"

// VirtualAccount is the payload for PaymentMethod "virtual_account"/"va".
// BankCode/VANumber are provider-issued identifiers a customer transfers
// to -- never raw bank account credentials of the customer's own account.
type VirtualAccount struct {
	BankCode  string     `json:"bank_code"`
	VANumber  string     `json:"va_number"`
	ExpiresAt *time.Time `json:"expires_at"`
}
