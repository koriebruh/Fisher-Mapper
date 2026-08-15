package rest

import (
	"github.com/gofiber/fiber/v2"

	"Fisher-Mapper/internal/domain/apperror"
	"Fisher-Mapper/internal/platform/tenantauth"
)

// tenantIDLocalsKey is the fiber c.Locals key tenantAuth stores the
// resolved tenant_id under -- unexported so only this package's handlers
// can read it (tenantIDFromLocals below), never a stray string literal
// elsewhere that could drift out of sync.
const tenantIDLocalsKey = "tenant_id"

// tenantAuth is the CRITICAL security fix's REST middleware: every payment/
// refund/payout route used to have no caller authentication at all, with
// tenant_id a self-asserted request-body field. Mirrors admin.go's adminAuth
// shape (one credential type, fail closed on any failure) but DB-backed
// (many tenants, not one shared admin secret) -- looks X-Api-Key up against
// tenant_api_keys and stores the resolved tenant_id in c.Locals for
// handlers to read via tenantIDFromLocals, never from the request body.
//
// Reuses X-Api-Key, the SAME header ratelimit.APIKeyOrIP already reads --
// one header now serves double duty (rate-limit bucketing AND
// authentication) rather than adding a third credential-shaped header to
// this API's surface.
func tenantAuth(store *tenantauth.Store) fiber.Handler {
	return func(c *fiber.Ctx) error {
		if store == nil {
			// Misconfiguration (PaymentService wired without a
			// TenantAuthStore) must fail closed, not silently skip auth --
			// same stance as adminAuth's apiKey == "" check.
			return writeError(c, apperror.New(apperror.CodeUnauthorized, "missing or invalid X-Api-Key header"))
		}

		apiKey := c.Get("X-Api-Key")
		tenantID, err := store.Resolve(c.Context(), apiKey)
		if err != nil {
			if apperror.CodeOf(err) == apperror.CodeUnauthorized {
				return writeError(c, apperror.New(apperror.CodeUnauthorized, "missing or invalid X-Api-Key header"))
			}
			// A genuine store error (DB unreachable, etc) -- still fails
			// the request, just reported as whatever apperror.CodeOf
			// resolves it to rather than masked as an auth failure.
			return writeError(c, err)
		}

		c.Locals(tenantIDLocalsKey, tenantID)
		return c.Next()
	}
}

// tenantIDFromLocals reads the tenant_id tenantAuth resolved for this
// request. Handlers use this as the ONLY source of truth for "which tenant
// is this" -- never a request-body field (see createPaymentRequest/
// createPayoutRequest's docs for why that field is gone).
func tenantIDFromLocals(c *fiber.Ctx) string {
	v, _ := c.Locals(tenantIDLocalsKey).(string)
	return v
}
