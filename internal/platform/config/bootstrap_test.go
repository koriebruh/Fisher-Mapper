package config

import (
	"os"
	"path/filepath"
	"testing"
)

// writeTempTOML writes content to a temp file distinct from every
// defaultBootstrap() value, so a passing test can only mean the TOML
// overlay actually ran — not that everything silently fell through to
// defaults.
func writeTempTOML(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp toml: %v", err)
	}
	return path
}

const distinctTOML = `
[postgres]
dsn = "postgres://x/y"

[redis]
addr = "redis.internal:6390"

[http]
port = 9091

[grpc]
port = 9092

[log]
level = "debug"
`

func TestLoad_TOMLOverlayOverridesDefaults(t *testing.T) {
	def := defaultBootstrap()
	path := writeTempTOML(t, distinctTOML)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Postgres.DSN != "postgres://x/y" || cfg.Postgres.DSN == def.Postgres.DSN {
		t.Errorf("Postgres.DSN = %q, want file value distinct from default %q", cfg.Postgres.DSN, def.Postgres.DSN)
	}
	if cfg.Redis.Addr != "redis.internal:6390" || cfg.Redis.Addr == def.Redis.Addr {
		t.Errorf("Redis.Addr = %q, want file value distinct from default %q", cfg.Redis.Addr, def.Redis.Addr)
	}
	if cfg.HTTP.Port != 9091 || cfg.HTTP.Port == def.HTTP.Port {
		t.Errorf("HTTP.Port = %d, want file value distinct from default %d", cfg.HTTP.Port, def.HTTP.Port)
	}
	if cfg.GRPC.Port != 9092 || cfg.GRPC.Port == def.GRPC.Port {
		t.Errorf("GRPC.Port = %d, want file value distinct from default %d", cfg.GRPC.Port, def.GRPC.Port)
	}
	if cfg.Log.Level != "debug" || cfg.Log.Level == def.Log.Level {
		t.Errorf("Log.Level = %q, want file value distinct from default %q", cfg.Log.Level, def.Log.Level)
	}
}

func TestLoad_EnvOverridesTOML(t *testing.T) {
	path := writeTempTOML(t, distinctTOML)

	t.Setenv(EnvHTTPPort, "9999")
	t.Setenv(EnvGRPCPort, "9998")
	t.Setenv(EnvLogLevel, "warn")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.HTTP.Port != 9999 {
		t.Errorf("HTTP.Port = %d, want env override 9999 (file had 9091)", cfg.HTTP.Port)
	}
	if cfg.GRPC.Port != 9998 {
		t.Errorf("GRPC.Port = %d, want env override 9998 (file had 9092)", cfg.GRPC.Port)
	}
	if cfg.Log.Level != "warn" {
		t.Errorf("Log.Level = %q, want env override %q (file had debug)", cfg.Log.Level, "warn")
	}
	// Values not overridden by env must still come from the file, proving
	// env-overlay only touches what it sets rather than resetting the
	// whole struct.
	if cfg.Postgres.DSN != "postgres://x/y" {
		t.Errorf("Postgres.DSN = %q, want unaffected file value %q", cfg.Postgres.DSN, "postgres://x/y")
	}
}

func TestLoad_MissingFileFallsBackToDefaults(t *testing.T) {
	cfg, err := Load(filepath.Join(t.TempDir(), "does-not-exist.toml"))
	if err != nil {
		t.Fatalf("Load with missing file should not error, got: %v", err)
	}

	def := defaultBootstrap()
	if cfg != def {
		t.Errorf("Load() with missing file = %+v, want defaults %+v", cfg, def)
	}
}

func TestLoad_InvalidEnvPortReturnsError(t *testing.T) {
	t.Setenv(EnvHTTPPort, "notanint")

	if _, err := Load(""); err == nil {
		t.Fatal("Load() with non-integer APP_HTTP_PORT should return an error, got nil")
	}
}
