// Package env implements secrets.Secrets by reading environment variables.
package env

import (
	"os"
	"strings"
)

// Secrets reads secrets from environment variables, optionally prefixed
// (e.g. Prefix "PROVIDER_MOCK_" turns GetSecret("api_key") into a lookup of
// PROVIDER_MOCK_API_KEY). Prefix may be empty.
type Secrets struct {
	Prefix string
}

func New(prefix string) Secrets {
	return Secrets{Prefix: prefix}
}

func (s Secrets) GetSecret(key string) string {
	name := s.Prefix + strings.ToUpper(key)
	return os.Getenv(name)
}
