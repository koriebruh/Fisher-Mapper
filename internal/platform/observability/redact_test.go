package observability

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"
)

func newTestLogger(buf *bytes.Buffer) *slog.Logger {
	base := slog.NewJSONHandler(buf, nil)
	return slog.New(NewRedactingHandler(base))
}

func TestRedactingHandler_MasksSensitiveKeysCaseInsensitive(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf)

	logger.Info("charge attempt",
		"card_number", "4242424242424242",
		"secret", "sk_live_abcdef",
		"CVV", "123",
		"Password", "p@ssw0rd",
		"token", "tok_abc123",
		"amount", int64(15000),
		"currency", "IDR",
	)

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("unmarshal log line: %v", err)
	}

	wantRedacted := []string{"card_number", "secret", "CVV", "Password", "token"}
	for _, key := range wantRedacted {
		if got := entry[key]; got != redactedValue {
			t.Errorf("entry[%q] = %v, want %q", key, got, redactedValue)
		}
	}

	// Non-sensitive fields must pass through untouched.
	if entry["amount"] != float64(15000) {
		t.Errorf(`entry["amount"] = %v, want 15000`, entry["amount"])
	}
	if entry["currency"] != "IDR" {
		t.Errorf(`entry["currency"] = %v, want "IDR"`, entry["currency"])
	}
}

func TestRedactingHandler_MasksAttrsBoundViaWith(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf)

	logger.With("password", "bound-secret").Info("login attempt")

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("unmarshal log line: %v", err)
	}

	if got := entry["password"]; got != redactedValue {
		t.Errorf(`entry["password"] = %v, want %q`, got, redactedValue)
	}
}

func TestRedactingHandler_MasksNestedGroupAttrs(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf)

	logger.Info("payload received",
		slog.Group("card",
			slog.String("card_number", "4242424242424242"),
			slog.String("brand", "visa"),
		),
	)

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("unmarshal log line: %v", err)
	}

	card, ok := entry["card"].(map[string]any)
	if !ok {
		t.Fatalf(`entry["card"] is not an object: %#v`, entry["card"])
	}
	if got := card["card_number"]; got != redactedValue {
		t.Errorf(`card["card_number"] = %v, want %q`, got, redactedValue)
	}
	if got := card["brand"]; got != "visa" {
		t.Errorf(`card["brand"] = %v, want "visa"`, got)
	}
}
