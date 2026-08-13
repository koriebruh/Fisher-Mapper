package config

import (
	"log/slog"
	"os"

	"github.com/joho/godotenv"
)

// DotEnvPath is the .env file LoadDotEnv looks for: the repo root, i.e. the
// current working directory the binary is expected to be launched from
// (same assumption Load's default "config.toml" path makes).
const DotEnvPath = ".env"

// LoadDotEnv loads a .env file from the repo root, if one is present,
// setting its keys into the process environment. It MUST run before
// Load/LoadDynamicSeed so that .env-defined values are visible to their
// existing os.LookupEnv-based override logic (overlayEnv in this package) —
// this function only populates the environment, it never touches that
// override logic itself.
//
// A missing .env file is a silent no-op, exactly like a missing
// config.toml (see Load's doc) — the hardcoded defaults plus config.toml
// plus real env vars are enough to run without one. Only a file that IS
// present but fails to parse is worth a warning.
func LoadDotEnv() {
	if _, err := os.Stat(DotEnvPath); err != nil {
		return
	}
	if err := godotenv.Load(DotEnvPath); err != nil {
		slog.Warn("config: .env file present but failed to parse, ignoring", "path", DotEnvPath, "error", err)
	}
}
