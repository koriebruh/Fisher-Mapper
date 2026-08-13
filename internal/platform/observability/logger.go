package observability

import (
	"log/slog"
	"os"
	"strings"
)

// NewLogger builds the process-wide slog.Logger: JSON output to stdout,
// wrapped with RedactingHandler so sensitive fields are masked at the
// handler level (i.e. no call site can accidentally bypass redaction).
func NewLogger(level string) *slog.Logger {
	base := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLevel(level),
	})
	return slog.New(NewRedactingHandler(base))
}

func parseLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
