package rest

import (
	"github.com/gofiber/fiber/v2"

	"Fisher-Mapper/internal/apperror"
	"Fisher-Mapper/internal/config"
)

// AdminDeps are the dependencies the Fase 4 admin config endpoints need.
// Cache is optional (nil disables the immediate-refresh-after-write
// convenience below, e.g. in cmd/server if it is ever run without a cache
// of its own -- the write still fully commits either way, propagation to
// the WORKER process's enforcement points always goes through that
// process's own periodic Cache.Run refresh regardless).
type AdminDeps struct {
	Store  *config.DynamicStore
	Cache  *config.Cache
	APIKey string
}

// adminAuth is the Fase 4 "RBAC sederhana" (plan stub-cheap item): a single
// static API key compared against the X-Admin-Key header. Deliberately not
// a full permission system — one shared credential, one role ("can change
// dynamic config") — per the plan's explicit "role check, bukan full
// permission system" framing.
func adminAuth(apiKey string) fiber.Handler {
	return func(c *fiber.Ctx) error {
		if apiKey == "" || c.Get("X-Admin-Key") != apiKey {
			return writeError(c, apperror.New(apperror.CodeUnauthorized, "admin: missing or invalid X-Admin-Key header"))
		}
		return c.Next()
	}
}

// RegisterAdminRoutes registers the Fase 4 dynamic-config admin surface:
//   - GET  /admin/config       -- list every current app_config key/value.
//   - PUT  /admin/config/:key  -- set one key, writing app_config +
//     app_config_audit in the SAME transaction (DynamicStore.SetWithAudit),
//     then best-effort refreshing this process's own Cache immediately
//     (the periodic background refresh is still the mechanism that matters
//     for cross-process propagation, e.g. to cmd/worker).
func RegisterAdminRoutes(app *fiber.App, deps AdminDeps) {
	group := app.Group("/admin", adminAuth(deps.APIKey))

	group.Get("/config", func(c *fiber.Ctx) error {
		values, err := deps.Store.GetAll(c.Context())
		if err != nil {
			return writeError(c, err)
		}
		return c.JSON(fiber.Map{"config": values})
	})

	group.Put("/config/:key", func(c *fiber.Ctx) error {
		var req struct {
			Value     string `json:"value"`
			UpdatedBy string `json:"updated_by"`
		}
		if err := c.BodyParser(&req); err != nil {
			return writeError(c, apperror.New(apperror.CodeValidation, "invalid JSON body"))
		}
		if req.UpdatedBy == "" {
			req.UpdatedBy = "admin_api"
		}

		key := c.Params("key")
		entry, err := deps.Store.SetWithAudit(c.Context(), key, req.Value, req.UpdatedBy)
		if err != nil {
			return writeError(c, err)
		}

		if deps.Cache != nil {
			deps.Cache.Refresh(c.Context())
		}

		return c.JSON(fiber.Map{
			"config_key": entry.ConfigKey,
			"old_value":  entry.OldValue,
			"new_value":  entry.NewValue,
			"changed_by": entry.ChangedBy,
			"changed_at": entry.ChangedAt,
		})
	})
}
