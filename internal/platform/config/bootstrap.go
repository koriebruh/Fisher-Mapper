// Package config holds the bootstrap configuration: the minimal set of
// values needed before any connection (Postgres, Redis) exists.
//
// Load order is explicit and MUST NOT be reordered:
//  1. hardcoded defaults (defaultBootstrap)
//  2. config.toml (repo root) overlay, if present
//  3. environment variable overrides
//
// Dynamic config (feature flags, retry counts, ...) lives in Postgres
// app_config and is intentionally NOT part of this package — it can only be
// loaded after a DB connection exists, which is a chicken-and-egg problem
// bootstrap config exists to avoid.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/BurntSushi/toml"
)

// Bootstrap is the set of values needed before any connection is formed.
type Bootstrap struct {
	Postgres Postgres
	Redis    Redis
	HTTP     HTTP
	GRPC     GRPC
	Log      Log
}

type Postgres struct {
	DSN string `toml:"dsn"`
}

type Redis struct {
	Addr string `toml:"addr"`
}

type HTTP struct {
	Port int `toml:"port"`
}

// GRPC is the Fase 6 gRPC transport's own listen port -- deliberately its
// own Bootstrap field (not folded into HTTP) since it is a second, fully
// independent listener in the same process (cmd/server runs both as
// separate oklog/run actors, see lifecycle.GRPCServerActor).
type GRPC struct {
	Port int `toml:"port"`
}

type Log struct {
	Level string `toml:"level"`
}

// fileConfig mirrors Bootstrap but with pointer fields so we can tell
// "not present in file" apart from "present with zero value" when overlaying
// onto the defaults.
type fileConfig struct {
	Postgres struct {
		DSN *string `toml:"dsn"`
	} `toml:"postgres"`
	Redis struct {
		Addr *string `toml:"addr"`
	} `toml:"redis"`
	HTTP struct {
		Port *int `toml:"port"`
	} `toml:"http"`
	GRPC struct {
		Port *int `toml:"port"`
	} `toml:"grpc"`
	Log struct {
		Level *string `toml:"level"`
	} `toml:"log"`
}

// defaultBootstrap must be enough on its own for the server to start against
// a local docker-compose stack with zero external configuration.
func defaultBootstrap() Bootstrap {
	return Bootstrap{
		Postgres: Postgres{
			DSN: "postgres://fisher:fisher@localhost:5432/fisher_mapper?sslmode=disable",
		},
		Redis: Redis{
			Addr: "localhost:6379",
		},
		HTTP: HTTP{
			Port: 8080,
		},
		GRPC: GRPC{
			Port: 9090,
		},
		Log: Log{
			Level: "info",
		},
	}
}

// Load builds the Bootstrap config following the fixed order:
// defaults -> config.toml (if present) -> env vars.
//
// path is the path to config.toml. If the file does not exist, that step is
// skipped silently (a missing bootstrap config file is not an error — the
// hardcoded defaults plus env vars are enough to run).
func Load(path string) (Bootstrap, error) {
	cfg := defaultBootstrap()

	if err := overlayFile(&cfg, path); err != nil {
		return Bootstrap{}, err
	}

	if err := overlayEnv(&cfg); err != nil {
		return Bootstrap{}, err
	}

	return cfg, nil
}

func overlayFile(cfg *Bootstrap, path string) error {
	if path == "" {
		return nil
	}
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("config: stat %s: %w", path, err)
	}

	var fc fileConfig
	if _, err := toml.DecodeFile(path, &fc); err != nil {
		return fmt.Errorf("config: decode %s: %w", path, err)
	}

	if fc.Postgres.DSN != nil {
		cfg.Postgres.DSN = *fc.Postgres.DSN
	}
	if fc.Redis.Addr != nil {
		cfg.Redis.Addr = *fc.Redis.Addr
	}
	if fc.HTTP.Port != nil {
		cfg.HTTP.Port = *fc.HTTP.Port
	}
	if fc.GRPC.Port != nil {
		cfg.GRPC.Port = *fc.GRPC.Port
	}
	if fc.Log.Level != nil {
		cfg.Log.Level = *fc.Log.Level
	}
	return nil
}

// Environment variable names. Kept explicit (no reflection-based automatic
// env binding) so the override set is grep-able and reviewable.
const (
	EnvPostgresDSN = "APP_POSTGRES_DSN"
	EnvRedisAddr   = "APP_REDIS_ADDR"
	EnvHTTPPort    = "APP_HTTP_PORT"
	EnvGRPCPort    = "APP_GRPC_PORT"
	EnvLogLevel    = "APP_LOG_LEVEL"
)

func overlayEnv(cfg *Bootstrap) error {
	if v, ok := lookupEnv(EnvPostgresDSN); ok {
		cfg.Postgres.DSN = v
	}
	if v, ok := lookupEnv(EnvRedisAddr); ok {
		cfg.Redis.Addr = v
	}
	if v, ok := lookupEnv(EnvHTTPPort); ok {
		port, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("config: env %s must be an integer: %w", EnvHTTPPort, err)
		}
		cfg.HTTP.Port = port
	}
	if v, ok := lookupEnv(EnvGRPCPort); ok {
		port, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("config: env %s must be an integer: %w", EnvGRPCPort, err)
		}
		cfg.GRPC.Port = port
	}
	if v, ok := lookupEnv(EnvLogLevel); ok {
		cfg.Log.Level = v
	}
	return nil
}

func lookupEnv(key string) (string, bool) {
	v, ok := os.LookupEnv(key)
	if !ok {
		return "", false
	}
	v = strings.TrimSpace(v)
	if v == "" {
		return "", false
	}
	return v, true
}
