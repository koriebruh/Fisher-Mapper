// Package secrets defines the secrets-manager interface used by provider
// credential lookups. This is a "stub cheap" item from the project plan:
// the interface exists from day one behind which callers (provider
// registration, Signer/Verifier construction) read credentials, so swapping
// the env-var implementation for Vault/AWS Secrets Manager later never
// touches caller code.
package secrets

// Secrets resolves a named secret to its value. The env implementation
// (internal/platform/secrets/env) simply reads an environment variable; other
// implementations may hit a network-backed secrets manager instead.
//
// GetSecret returns "" if the key is not found — callers that require a
// secret to be present must check for an empty result themselves. This
// mirrors os.Getenv's zero-value-on-missing behavior, which the env
// implementation delegates directly to.
type Secrets interface {
	GetSecret(key string) string
}
