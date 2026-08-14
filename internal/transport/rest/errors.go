package rest

import (
	"log/slog"

	"github.com/gofiber/fiber/v2"

	"Fisher-Mapper/internal/domain/apperror"
)

// httpStatusFor maps the transport-agnostic apperror taxonomy to an HTTP
// status code. This is the ONLY place that mapping happens — the
// domain/service layer never imports fiber or constructs an HTTP status
// itself, so the same taxonomy can be mapped to gRPC status codes later
// (Phase 6) without touching business logic.
func httpStatusFor(code apperror.Code) int {
	switch code {
	case apperror.CodeValidation:
		return fiber.StatusBadRequest
	case apperror.CodeUnauthorized:
		return fiber.StatusUnauthorized
	case apperror.CodeNotFound:
		return fiber.StatusNotFound
	case apperror.CodeIdempotencyConflict:
		return fiber.StatusConflict
	case apperror.CodeIdempotencyInProgress:
		return fiber.StatusConflict
	case apperror.CodeDuplicateEvent:
		return fiber.StatusConflict
	case apperror.CodeTerminalState, apperror.CodeInvalidTransition, apperror.CodeStaleEvent:
		return fiber.StatusConflict
	case apperror.CodeRefundLimitExceeded:
		return fiber.StatusConflict
	case apperror.CodeProviderNotRegistered:
		return fiber.StatusBadRequest
	case apperror.CodeProviderTimeout:
		return fiber.StatusGatewayTimeout
	case apperror.CodeProviderError:
		return fiber.StatusBadGateway
	case apperror.CodeProviderDisabled:
		return fiber.StatusServiceUnavailable
	default:
		return fiber.StatusInternalServerError
	}
}

// writeError renders err as a JSON error body with the mapped HTTP status.
// The response body only ever carries the stable Code plus a message —
// never the raw wrapped cause (which may contain internal detail not meant
// for API callers); the cause is logged server-side instead.
func writeError(c *fiber.Ctx, err error) error {
	code := apperror.CodeOf(err)
	status := httpStatusFor(code)

	message := err.Error()
	if status >= fiber.StatusInternalServerError {
		// 5xx bodies never carry the raw error text (which may embed a DB
		// error, a wrapped provider error, etc.) — that detail goes to the
		// log only.
		slog.Error("request failed", "error", err, "code", code, apperror.LogAttr(err))
		message = "internal error"
	}

	return c.Status(status).JSON(fiber.Map{
		"error": fiber.Map{
			"code":    code,
			"message": message,
		},
	})
}
