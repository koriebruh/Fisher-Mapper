package payload

// Card is the payload for PaymentMethod "card"/"cc". MaskedPAN is
// display-only (e.g. "411111******1111"), never a full PAN -- see
// provider.go's PCI-DSS scope note: no field in this codebase ever carries
// raw PAN/CVV/track data. RedirectURL is set instead when the provider
// requires a 3DS/hosted-page redirect step, nil otherwise.
type Card struct {
	MaskedPAN   *string `json:"masked_pan"`
	RedirectURL *string `json:"redirect_url"`
}
