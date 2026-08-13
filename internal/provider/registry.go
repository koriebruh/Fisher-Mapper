package provider

import (
	"fmt"
	"sort"
	"sync"

	"Fisher-Mapper/internal/domain/apperror"
)

// Registry holds explicitly registered providers by name.
//
// Per the plan's "Larangan blank import" rule, registration is done by
// calling Register from an explicit bootstrap function (see
// internal/platform/bootstrap/register.go), executed in order from main() — never
// via `import _ "package"` for an init() side effect.
type Registry struct {
	mu        sync.RWMutex
	providers map[string]Provider
}

func NewRegistry() *Registry {
	return &Registry{providers: make(map[string]Provider)}
}

// Register adds p under name. Registering the same name twice is a
// programming error (bootstrap wiring bug), so it panics rather than
// silently overwriting — this only ever runs once, at startup, from
// explicit code the developer controls.
func (r *Registry) Register(name string, p Provider) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.providers[name]; exists {
		panic(fmt.Sprintf("provider: %q already registered", name))
	}
	r.providers[name] = p
}

func (r *Registry) Get(name string) (Provider, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.providers[name]
	if !ok {
		return nil, apperror.New(apperror.CodeProviderNotRegistered, fmt.Sprintf("provider %q is not registered", name))
	}
	return p, nil
}

// Names returns the sorted list of registered provider names, mainly for
// diagnostics/tests.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.providers))
	for name := range r.providers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
