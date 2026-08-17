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
	Postgres  Postgres
	Redis     Redis
	HTTP      HTTP
	GRPC      GRPC
	Log       Log
	Service   Service
	Ratelimit Ratelimit
	Worker    Worker
	Server    Server
	CORS      CORS
}

// CORS configures cmd/server's REST CORS middleware. Bootstrap config (not
// dynamic): which origins/headers a browser is allowed to call this API from
// is a deployment-time decision, not something that should flip live without
// a restart. AllowOrigins/AllowMethods/AllowHeaders/ExposeHeaders are
// comma-separated strings on purpose -- that's the exact shape
// github.com/gofiber/fiber/v2/middleware/cors.Config's fields already take,
// so cmd/server passes these straight through with zero parsing.
type CORS struct {
	Enabled          bool   `toml:"enabled"`
	AllowOrigins     string `toml:"allow_origins"`
	AllowMethods     string `toml:"allow_methods"`
	AllowHeaders     string `toml:"allow_headers"`
	ExposeHeaders    string `toml:"expose_headers"`
	AllowCredentials bool   `toml:"allow_credentials"`
	MaxAgeSeconds    int    `toml:"max_age_seconds"`
}

// Server holds cmd/server's own process-tuning knobs -- the fiber-vs-worker
// counterpart of Worker above, same "bootstrap config, not a feature flag"
// reasoning. ShutdownTimeoutSeconds feeds BOTH the fiber actor and the gRPC
// actor (see cmd/server/main.go) -- they used to be two independently
// hardcoded 5*time.Second literals with a comment merely asserting they
// matched; a single field makes that actually guaranteed rather than
// coincidental.
type Server struct {
	ShutdownTimeoutSeconds              int `toml:"shutdown_timeout_seconds"`
	DynamicConfigRefreshIntervalSeconds int `toml:"dynamic_config_refresh_interval_seconds"`
	MetricsPollIntervalSeconds          int `toml:"metrics_poll_interval_seconds"`
}

// Worker holds cmd/worker's own process-tuning knobs -- all of it is
// bootstrap config (process tuning, read once at startup to construct
// resilience primitives/the asynq server/pollers), not dynamic config: none
// of it is a feature flag that should flip live without a restart. TOML has
// no native duration type, so every interval/timeout is seconds-as-integer,
// converted to time.Duration at the one call site that needs it
// (cmd/worker/main.go).
type Worker struct {
	// BreakerFailureThreshold/BreakerCooldownSeconds size the per-provider
	// circuit breaker (internal/resilience/circuitbreaker).
	BreakerFailureThreshold int `toml:"breaker_failure_threshold"`
	BreakerCooldownSeconds  int `toml:"breaker_cooldown_seconds"`
	// BulkheadCapacityPerProvider caps concurrent in-flight provider calls
	// per provider, so one slow PJP can't starve the others.
	BulkheadCapacityPerProvider int `toml:"bulkhead_capacity_per_provider"`
	// AsynqConcurrency is how many charge/refund/payout tasks this worker
	// processes at once.
	AsynqConcurrency int `toml:"asynq_concurrency"`
	// RelayBaseIntervalSeconds/RelayMaxIntervalSeconds/RelayBatchSize tune
	// the outbox relay's poll backoff and how many rows it dispatches per
	// poll (internal/messaging/outbox).
	RelayBaseIntervalSeconds int `toml:"relay_base_interval_seconds"`
	RelayMaxIntervalSeconds  int `toml:"relay_max_interval_seconds"`
	RelayBatchSize           int `toml:"relay_batch_size"`
	// RedisHealthIntervalSeconds governs how often the switching queue
	// client re-checks Redis reachability to flip between the durable asynq
	// client and the in-memory fallback.
	RedisHealthIntervalSeconds int `toml:"redis_health_interval_seconds"`
	// DynamicConfigRefreshIntervalSeconds is how often this process
	// refreshes its app_config cache in the background.
	DynamicConfigRefreshIntervalSeconds int `toml:"dynamic_config_refresh_interval_seconds"`
	// ReconciliationPollIntervalSeconds/ReconciliationStuckThresholdSeconds
	// tune the reconciliation job: how often it runs, and how long a
	// payment must have sat in "processing" before it's touched.
	ReconciliationPollIntervalSeconds   int `toml:"reconciliation_poll_interval_seconds"`
	ReconciliationStuckThresholdSeconds int `toml:"reconciliation_stuck_threshold_seconds"`
	// MetricsPollIntervalSeconds governs the DB-pool-stats +
	// terminal_failures-depth poller actor.
	MetricsPollIntervalSeconds int `toml:"metrics_poll_interval_seconds"`
	// MetricsPort is this process's own GET /metrics listener port --
	// deliberately its own field (not cfg.HTTP.Port, which cmd/server uses
	// for the same purpose) since cmd/worker has no fiber app of its own and
	// normally runs alongside cmd/server on the same host (see the
	// Makefile's `make run`/`make run-worker`), so it needs a port that
	// can't collide with cmd/server's.
	MetricsPort string `toml:"metrics_port"`
}

// Service names this process for the OTel resource attribute and log
// fields. cmd/worker does not get its own key here -- it derives
// "<name>-worker" from this one (see cmd/worker/main.go), so the two
// process names can't drift apart the way two independent config keys
// could.
type Service struct {
	Name string `toml:"name"`
}

// Ratelimit's RatePerSecond/Burst are bootstrap-only (process tuning, not a
// feature flag): they are consumed once at startup to construct the
// resilience/ratelimit.Limiter and never re-read live. This is the SAME
// [ratelimit] TOML table that also has an `enabled` key, but that key is
// dynamic-config-seeded (see LoadDynamicSeed/DynamicSeed.RateLimitEnabled in
// dynamic.go) and lives in app_config after first boot -- overlayFile below
// only reads rate_per_second/burst from this table and LoadDynamicSeed only
// reads enabled, so the two loaders coexist on one TOML section without
// collision.
type Ratelimit struct {
	RatePerSecond int `toml:"rate_per_second"`
	Burst         int `toml:"burst"`
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
	Service struct {
		Name *string `toml:"name"`
	} `toml:"service"`
	Ratelimit struct {
		RatePerSecond *int `toml:"rate_per_second"`
		Burst         *int `toml:"burst"`
	} `toml:"ratelimit"`
	Worker struct {
		BreakerFailureThreshold             *int    `toml:"breaker_failure_threshold"`
		BreakerCooldownSeconds              *int    `toml:"breaker_cooldown_seconds"`
		BulkheadCapacityPerProvider         *int    `toml:"bulkhead_capacity_per_provider"`
		AsynqConcurrency                    *int    `toml:"asynq_concurrency"`
		RelayBaseIntervalSeconds            *int    `toml:"relay_base_interval_seconds"`
		RelayMaxIntervalSeconds             *int    `toml:"relay_max_interval_seconds"`
		RelayBatchSize                      *int    `toml:"relay_batch_size"`
		RedisHealthIntervalSeconds          *int    `toml:"redis_health_interval_seconds"`
		DynamicConfigRefreshIntervalSeconds *int    `toml:"dynamic_config_refresh_interval_seconds"`
		ReconciliationPollIntervalSeconds   *int    `toml:"reconciliation_poll_interval_seconds"`
		ReconciliationStuckThresholdSeconds *int    `toml:"reconciliation_stuck_threshold_seconds"`
		MetricsPollIntervalSeconds          *int    `toml:"metrics_poll_interval_seconds"`
		MetricsPort                         *string `toml:"metrics_port"`
	} `toml:"worker"`
	Server struct {
		ShutdownTimeoutSeconds              *int `toml:"shutdown_timeout_seconds"`
		DynamicConfigRefreshIntervalSeconds *int `toml:"dynamic_config_refresh_interval_seconds"`
		MetricsPollIntervalSeconds          *int `toml:"metrics_poll_interval_seconds"`
	} `toml:"server"`
	CORS struct {
		Enabled          *bool   `toml:"enabled"`
		AllowOrigins     *string `toml:"allow_origins"`
		AllowMethods     *string `toml:"allow_methods"`
		AllowHeaders     *string `toml:"allow_headers"`
		ExposeHeaders    *string `toml:"expose_headers"`
		AllowCredentials *bool   `toml:"allow_credentials"`
		MaxAgeSeconds    *int    `toml:"max_age_seconds"`
	} `toml:"cors"`
}

// defaultPostgresDSN matches docker-compose's throwaway local credentials,
// not a real secret -- gosec's hardcoded-credential heuristic can't tell the
// difference from a real password-in-URL. Pulled into its own named
// constant (rather than suppressing the field inline) because gosec's G101
// reports the position of the enclosing composite literal, not the string
// literal itself, so a same-line suppression on the field doesn't line up.
// #nosec (not //nolint:gosec) because golangci-lint's gosec linter honors
// the same native directive the standalone `gosec` CLI (CI's separate
// security job) requires -- one suppression comment works for both.
const defaultPostgresDSN = "postgres://fisher:fisher@localhost:5432/fisher_mapper?sslmode=disable" //#nosec G101 -- local dev default, matches docker-compose.yml

// defaultBootstrap must be enough on its own for the server to start against
// a local docker-compose stack with zero external configuration.
func defaultBootstrap() Bootstrap {
	return Bootstrap{
		Postgres: Postgres{
			DSN: defaultPostgresDSN,
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
		Service: Service{
			Name: "fisher-mapper",
		},
		Ratelimit: Ratelimit{
			RatePerSecond: 20,
			Burst:         40,
		},
		Worker: Worker{
			BreakerFailureThreshold:             5,
			BreakerCooldownSeconds:              30,
			BulkheadCapacityPerProvider:         4,
			AsynqConcurrency:                    10,
			RelayBaseIntervalSeconds:            2,
			RelayMaxIntervalSeconds:             30,
			RelayBatchSize:                      50,
			RedisHealthIntervalSeconds:          2,
			DynamicConfigRefreshIntervalSeconds: 5,
			ReconciliationPollIntervalSeconds:   15,
			ReconciliationStuckThresholdSeconds: 60,
			MetricsPollIntervalSeconds:          15,
			MetricsPort:                         "9101",
		},
		Server: Server{
			ShutdownTimeoutSeconds:              5,
			DynamicConfigRefreshIntervalSeconds: 30,
			MetricsPollIntervalSeconds:          15,
		},
		CORS: CORS{
			Enabled:          true,
			AllowOrigins:     "*",
			AllowMethods:     "GET,POST,PUT,PATCH,DELETE,HEAD,OPTIONS",
			AllowHeaders:     "Origin,Content-Type,Accept,Authorization,X-Api-Key,Idempotency-Key,X-Admin-Key",
			ExposeHeaders:    "",
			AllowCredentials: false,
			MaxAgeSeconds:    300,
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
	if fc.Service.Name != nil {
		cfg.Service.Name = *fc.Service.Name
	}
	if fc.Ratelimit.RatePerSecond != nil {
		cfg.Ratelimit.RatePerSecond = *fc.Ratelimit.RatePerSecond
	}
	if fc.Ratelimit.Burst != nil {
		cfg.Ratelimit.Burst = *fc.Ratelimit.Burst
	}
	if fc.Worker.BreakerFailureThreshold != nil {
		cfg.Worker.BreakerFailureThreshold = *fc.Worker.BreakerFailureThreshold
	}
	if fc.Worker.BreakerCooldownSeconds != nil {
		cfg.Worker.BreakerCooldownSeconds = *fc.Worker.BreakerCooldownSeconds
	}
	if fc.Worker.BulkheadCapacityPerProvider != nil {
		cfg.Worker.BulkheadCapacityPerProvider = *fc.Worker.BulkheadCapacityPerProvider
	}
	if fc.Worker.AsynqConcurrency != nil {
		cfg.Worker.AsynqConcurrency = *fc.Worker.AsynqConcurrency
	}
	if fc.Worker.RelayBaseIntervalSeconds != nil {
		cfg.Worker.RelayBaseIntervalSeconds = *fc.Worker.RelayBaseIntervalSeconds
	}
	if fc.Worker.RelayMaxIntervalSeconds != nil {
		cfg.Worker.RelayMaxIntervalSeconds = *fc.Worker.RelayMaxIntervalSeconds
	}
	if fc.Worker.RelayBatchSize != nil {
		cfg.Worker.RelayBatchSize = *fc.Worker.RelayBatchSize
	}
	if fc.Worker.RedisHealthIntervalSeconds != nil {
		cfg.Worker.RedisHealthIntervalSeconds = *fc.Worker.RedisHealthIntervalSeconds
	}
	if fc.Worker.DynamicConfigRefreshIntervalSeconds != nil {
		cfg.Worker.DynamicConfigRefreshIntervalSeconds = *fc.Worker.DynamicConfigRefreshIntervalSeconds
	}
	if fc.Worker.ReconciliationPollIntervalSeconds != nil {
		cfg.Worker.ReconciliationPollIntervalSeconds = *fc.Worker.ReconciliationPollIntervalSeconds
	}
	if fc.Worker.ReconciliationStuckThresholdSeconds != nil {
		cfg.Worker.ReconciliationStuckThresholdSeconds = *fc.Worker.ReconciliationStuckThresholdSeconds
	}
	if fc.Worker.MetricsPollIntervalSeconds != nil {
		cfg.Worker.MetricsPollIntervalSeconds = *fc.Worker.MetricsPollIntervalSeconds
	}
	if fc.Worker.MetricsPort != nil {
		cfg.Worker.MetricsPort = *fc.Worker.MetricsPort
	}
	if fc.Server.ShutdownTimeoutSeconds != nil {
		cfg.Server.ShutdownTimeoutSeconds = *fc.Server.ShutdownTimeoutSeconds
	}
	if fc.Server.DynamicConfigRefreshIntervalSeconds != nil {
		cfg.Server.DynamicConfigRefreshIntervalSeconds = *fc.Server.DynamicConfigRefreshIntervalSeconds
	}
	if fc.Server.MetricsPollIntervalSeconds != nil {
		cfg.Server.MetricsPollIntervalSeconds = *fc.Server.MetricsPollIntervalSeconds
	}
	if fc.CORS.Enabled != nil {
		cfg.CORS.Enabled = *fc.CORS.Enabled
	}
	if fc.CORS.AllowOrigins != nil {
		cfg.CORS.AllowOrigins = *fc.CORS.AllowOrigins
	}
	if fc.CORS.AllowMethods != nil {
		cfg.CORS.AllowMethods = *fc.CORS.AllowMethods
	}
	if fc.CORS.AllowHeaders != nil {
		cfg.CORS.AllowHeaders = *fc.CORS.AllowHeaders
	}
	if fc.CORS.ExposeHeaders != nil {
		cfg.CORS.ExposeHeaders = *fc.CORS.ExposeHeaders
	}
	if fc.CORS.AllowCredentials != nil {
		cfg.CORS.AllowCredentials = *fc.CORS.AllowCredentials
	}
	if fc.CORS.MaxAgeSeconds != nil {
		cfg.CORS.MaxAgeSeconds = *fc.CORS.MaxAgeSeconds
	}
	return nil
}

// Environment variable names. Kept explicit (no reflection-based automatic
// env binding) so the override set is grep-able and reviewable.
const (
	EnvPostgresDSN            = "APP_POSTGRES_DSN"
	EnvRedisAddr              = "APP_REDIS_ADDR"
	EnvHTTPPort               = "APP_HTTP_PORT"
	EnvGRPCPort               = "APP_GRPC_PORT"
	EnvLogLevel               = "APP_LOG_LEVEL"
	EnvServiceName            = "APP_SERVICE_NAME"
	EnvRatelimitRatePerSecond = "APP_RATELIMIT_RATE_PER_SECOND"
	EnvRatelimitBurst         = "APP_RATELIMIT_BURST"

	EnvWorkerBreakerFailureThreshold             = "APP_WORKER_BREAKER_FAILURE_THRESHOLD"
	EnvWorkerBreakerCooldownSeconds              = "APP_WORKER_BREAKER_COOLDOWN_SECONDS"
	EnvWorkerBulkheadCapacityPerProvider         = "APP_WORKER_BULKHEAD_CAPACITY_PER_PROVIDER"
	EnvWorkerAsynqConcurrency                    = "APP_WORKER_ASYNQ_CONCURRENCY"
	EnvWorkerRelayBaseIntervalSeconds            = "APP_WORKER_RELAY_BASE_INTERVAL_SECONDS"
	EnvWorkerRelayMaxIntervalSeconds             = "APP_WORKER_RELAY_MAX_INTERVAL_SECONDS"
	EnvWorkerRelayBatchSize                      = "APP_WORKER_RELAY_BATCH_SIZE"
	EnvWorkerRedisHealthIntervalSeconds          = "APP_WORKER_REDIS_HEALTH_INTERVAL_SECONDS"
	EnvWorkerDynamicConfigRefreshIntervalSeconds = "APP_WORKER_DYNAMIC_CONFIG_REFRESH_INTERVAL_SECONDS"
	EnvWorkerReconciliationPollIntervalSeconds   = "APP_WORKER_RECONCILIATION_POLL_INTERVAL_SECONDS"
	EnvWorkerReconciliationStuckThresholdSeconds = "APP_WORKER_RECONCILIATION_STUCK_THRESHOLD_SECONDS"
	EnvWorkerMetricsPollIntervalSeconds          = "APP_WORKER_METRICS_POLL_INTERVAL_SECONDS"
	// EnvWorkerMetricsPort keeps the SAME env var name the pre-Bootstrap
	// code used (os.Getenv("APP_WORKER_METRICS_PORT")) so existing
	// deployments/.env files that already set it keep working unchanged.
	EnvWorkerMetricsPort = "APP_WORKER_METRICS_PORT"

	EnvServerShutdownTimeoutSeconds              = "APP_SERVER_SHUTDOWN_TIMEOUT_SECONDS"
	EnvServerDynamicConfigRefreshIntervalSeconds = "APP_SERVER_DYNAMIC_CONFIG_REFRESH_INTERVAL_SECONDS"
	EnvServerMetricsPollIntervalSeconds          = "APP_SERVER_METRICS_POLL_INTERVAL_SECONDS"

	EnvCORSEnabled          = "APP_CORS_ENABLED"
	EnvCORSAllowOrigins     = "APP_CORS_ALLOW_ORIGINS"
	EnvCORSAllowMethods     = "APP_CORS_ALLOW_METHODS"
	EnvCORSAllowHeaders     = "APP_CORS_ALLOW_HEADERS"
	EnvCORSExposeHeaders    = "APP_CORS_EXPOSE_HEADERS"
	EnvCORSAllowCredentials = "APP_CORS_ALLOW_CREDENTIALS" //#nosec G101 -- env var NAME, not a credential value: gosec's name-pattern match on "credentials" is a false positive here
	EnvCORSMaxAgeSeconds    = "APP_CORS_MAX_AGE_SECONDS"
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
	if v, ok := lookupEnv(EnvServiceName); ok {
		cfg.Service.Name = v
	}
	if v, ok := lookupEnv(EnvRatelimitRatePerSecond); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("config: env %s must be an integer: %w", EnvRatelimitRatePerSecond, err)
		}
		cfg.Ratelimit.RatePerSecond = n
	}
	if v, ok := lookupEnv(EnvRatelimitBurst); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("config: env %s must be an integer: %w", EnvRatelimitBurst, err)
		}
		cfg.Ratelimit.Burst = n
	}
	if err := overlayWorkerEnv(cfg); err != nil {
		return err
	}
	if err := overlayServerEnv(cfg); err != nil {
		return err
	}
	if err := overlayCORSEnv(cfg); err != nil {
		return err
	}
	return nil
}

// overlayCORSEnv is split out of overlayEnv for the same reason
// overlayWorkerEnv/overlayServerEnv are: keeps overlayEnv's line count from
// ballooning. CORS mixes string/bool/int fields (unlike Worker/Server's
// all-int tables), so it stays a flat function rather than a table loop.
func overlayCORSEnv(cfg *Bootstrap) error {
	if v, ok := lookupEnv(EnvCORSEnabled); ok {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("config: env %s must be a bool: %w", EnvCORSEnabled, err)
		}
		cfg.CORS.Enabled = b
	}
	if v, ok := lookupEnv(EnvCORSAllowOrigins); ok {
		cfg.CORS.AllowOrigins = v
	}
	if v, ok := lookupEnv(EnvCORSAllowMethods); ok {
		cfg.CORS.AllowMethods = v
	}
	if v, ok := lookupEnv(EnvCORSAllowHeaders); ok {
		cfg.CORS.AllowHeaders = v
	}
	if v, ok := lookupEnv(EnvCORSExposeHeaders); ok {
		cfg.CORS.ExposeHeaders = v
	}
	if v, ok := lookupEnv(EnvCORSAllowCredentials); ok {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("config: env %s must be a bool: %w", EnvCORSAllowCredentials, err)
		}
		cfg.CORS.AllowCredentials = b
	}
	if v, ok := lookupEnv(EnvCORSMaxAgeSeconds); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("config: env %s must be an integer: %w", EnvCORSMaxAgeSeconds, err)
		}
		cfg.CORS.MaxAgeSeconds = n
	}
	return nil
}

// overlayWorkerEnv is split out of overlayEnv purely to keep that function's
// line count from ballooning -- same overlay semantics (env var present and
// non-blank wins over whatever cfg already holds).
func overlayWorkerEnv(cfg *Bootstrap) error {
	intEnvs := []struct {
		name string
		dst  *int
	}{
		{EnvWorkerBreakerFailureThreshold, &cfg.Worker.BreakerFailureThreshold},
		{EnvWorkerBreakerCooldownSeconds, &cfg.Worker.BreakerCooldownSeconds},
		{EnvWorkerBulkheadCapacityPerProvider, &cfg.Worker.BulkheadCapacityPerProvider},
		{EnvWorkerAsynqConcurrency, &cfg.Worker.AsynqConcurrency},
		{EnvWorkerRelayBaseIntervalSeconds, &cfg.Worker.RelayBaseIntervalSeconds},
		{EnvWorkerRelayMaxIntervalSeconds, &cfg.Worker.RelayMaxIntervalSeconds},
		{EnvWorkerRelayBatchSize, &cfg.Worker.RelayBatchSize},
		{EnvWorkerRedisHealthIntervalSeconds, &cfg.Worker.RedisHealthIntervalSeconds},
		{EnvWorkerDynamicConfigRefreshIntervalSeconds, &cfg.Worker.DynamicConfigRefreshIntervalSeconds},
		{EnvWorkerReconciliationPollIntervalSeconds, &cfg.Worker.ReconciliationPollIntervalSeconds},
		{EnvWorkerReconciliationStuckThresholdSeconds, &cfg.Worker.ReconciliationStuckThresholdSeconds},
		{EnvWorkerMetricsPollIntervalSeconds, &cfg.Worker.MetricsPollIntervalSeconds},
	}
	for _, e := range intEnvs {
		if v, ok := lookupEnv(e.name); ok {
			n, err := strconv.Atoi(v)
			if err != nil {
				return fmt.Errorf("config: env %s must be an integer: %w", e.name, err)
			}
			*e.dst = n
		}
	}
	if v, ok := lookupEnv(EnvWorkerMetricsPort); ok {
		cfg.Worker.MetricsPort = v
	}
	return nil
}

func overlayServerEnv(cfg *Bootstrap) error {
	intEnvs := []struct {
		name string
		dst  *int
	}{
		{EnvServerShutdownTimeoutSeconds, &cfg.Server.ShutdownTimeoutSeconds},
		{EnvServerDynamicConfigRefreshIntervalSeconds, &cfg.Server.DynamicConfigRefreshIntervalSeconds},
		{EnvServerMetricsPollIntervalSeconds, &cfg.Server.MetricsPollIntervalSeconds},
	}
	for _, e := range intEnvs {
		if v, ok := lookupEnv(e.name); ok {
			n, err := strconv.Atoi(v)
			if err != nil {
				return fmt.Errorf("config: env %s must be an integer: %w", e.name, err)
			}
			*e.dst = n
		}
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
