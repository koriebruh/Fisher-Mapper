package observability

import (
	"context"
	"log/slog"
	"strings"
)

// sensitiveKeys lists attribute keys (case-insensitive, exact match) whose
// values must never reach a log sink in cleartext. This rule is wired into
// the logger from the very start of Phase 1 — deliberately, before any
// sensitive payload (card data, secrets, tokens) exists anywhere in the
// codebase — so no later feature can "forget" to redact.
var sensitiveKeys = map[string]struct{}{
	"card_number": {},
	"cardnumber":  {},
	"secret":      {},
	"password":    {},
	"token":       {},
	"cvv":         {},
}

const redactedValue = "[REDACTED]"

func isSensitiveKey(key string) bool {
	_, ok := sensitiveKeys[strings.ToLower(key)]
	return ok
}

// redactAttr returns a with its value replaced by redactedValue if its key
// matches the sensitive-key list, recursing into slog groups so nested
// attrs (e.g. logger.With("payment", slog.Group("card", "card_number", ...)))
// are covered too.
func redactAttr(a slog.Attr) slog.Attr {
	if isSensitiveKey(a.Key) {
		return slog.String(a.Key, redactedValue)
	}
	if a.Value.Kind() == slog.KindGroup {
		group := a.Value.Group()
		redacted := make([]slog.Attr, len(group))
		for i, ga := range group {
			redacted[i] = redactAttr(ga)
		}
		return slog.Attr{Key: a.Key, Value: slog.GroupValue(redacted...)}
	}
	return a
}

// RedactingHandler wraps an slog.Handler and masks sensitive attribute
// values (card_number, secret, password, token, cvv — case-insensitive key
// match) before they reach the wrapped handler's formatter/sink.
type RedactingHandler struct {
	next slog.Handler
}

// NewRedactingHandler wraps next with redaction. next is typically a
// slog.JSONHandler or slog.TextHandler writing to the process's log sink.
func NewRedactingHandler(next slog.Handler) *RedactingHandler {
	return &RedactingHandler{next: next}
}

func (h *RedactingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *RedactingHandler) Handle(ctx context.Context, r slog.Record) error {
	nr := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	r.Attrs(func(a slog.Attr) bool {
		nr.AddAttrs(redactAttr(a))
		return true
	})
	return h.next.Handle(ctx, nr)
}

func (h *RedactingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	redacted := make([]slog.Attr, len(attrs))
	for i, a := range attrs {
		redacted[i] = redactAttr(a)
	}
	return &RedactingHandler{next: h.next.WithAttrs(redacted)}
}

func (h *RedactingHandler) WithGroup(name string) slog.Handler {
	return &RedactingHandler{next: h.next.WithGroup(name)}
}
