// Package payload holds the per-payment-method response shapes a PJP
// returns from a Charge call (QRIS string, VA number, card redirect, ...),
// kept in their own package (rather than scattered fields on
// provider.ChargeResponse) so a new payment method's payload is one new file
// here, not a growing pile of optional fields on a shared struct.
package payload

// MethodPayload discriminates by Method (mirrors provider.ChargeRequest's
// PaymentMethod string) with one nullable pointer field per method. Only the
// field matching Method is non-nil, rest are nil -> JSON null.
type MethodPayload struct {
	Method         string          `json:"method"`
	QRIS           *QRIS           `json:"qris"`
	VirtualAccount *VirtualAccount `json:"virtual_account"`
	Card           *Card           `json:"card"`
	EWallet        *EWallet        `json:"ewallet"`
}
