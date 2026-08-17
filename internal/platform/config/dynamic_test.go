package config

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeConfigSource is a configSource test double that can be told to fail
// on command -- lets the fallback-on-refresh-failure behavior (the entire
// reason Cache exists, per the plan: "Kalau DB gak bisa diakses pas
// refresh, jatuh balik ke cache terakhir yang valid, bukan error total") be
// exercised without a live Postgres connection. Guarded by a mutex since
// TestCache_Run_ContinuesTickingAfterARefreshFailure mutates it from the
// test goroutine while Cache.Run reads it from a background goroutine.
type fakeConfigSource struct {
	mu      sync.Mutex
	values  map[string]string
	fail    bool
	calls   int
	lastErr error
}

func (f *fakeConfigSource) GetAll(_ context.Context) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.fail {
		if f.lastErr != nil {
			return nil, f.lastErr
		}
		return nil, errors.New("fake: postgres unreachable")
	}
	// Return a copy so the test and the cache never alias the same map.
	out := make(map[string]string, len(f.values))
	for k, v := range f.values {
		out[k] = v
	}
	return out, nil
}

func (f *fakeConfigSource) setFail(fail bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail = fail
}

func (f *fakeConfigSource) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func TestCache_Load_PopulatesValues(t *testing.T) {
	src := &fakeConfigSource{values: map[string]string{"provider.mock.enabled": "true"}}
	c := NewCache(src, time.Hour)

	if err := c.Load(context.Background()); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !c.ProviderEnabled("mock") {
		t.Error("ProviderEnabled(mock) = false after Load with provider.mock.enabled=true, want true")
	}
}

// TestCache_Load_FailureAtStartupIsNotSwallowed is the advisor-flagged
// distinction from refresh: at t=0 there is no last-known-good snapshot to
// fall back to, so Load must propagate the error (the caller -- cmd/worker
// -- decides to fail startup on it, consistent with db.NewPool already
// hard-failing on its own Ping).
func TestCache_Load_FailureAtStartupIsNotSwallowed(t *testing.T) {
	src := &fakeConfigSource{fail: true}
	c := NewCache(src, time.Hour)

	if err := c.Load(context.Background()); err == nil {
		t.Fatal("Load with a failing source = nil error, want an error (no last-known-good snapshot exists yet)")
	}
}

// TestCache_Refresh_FallsBackToLastKnownGoodOnFailure is the central Fase 4
// mandatory behavior: a refresh that fails (DB unreachable) must NOT clear
// or corrupt the cache -- reads must keep returning whatever the last
// successful load/refresh populated, and the failure must not propagate as
// an error/panic to the caller (Refresh has no error return at all).
func TestCache_Refresh_FallsBackToLastKnownGoodOnFailure(t *testing.T) {
	src := &fakeConfigSource{values: map[string]string{"provider.mock.enabled": "false"}}
	c := NewCache(src, time.Hour)
	if err := c.Load(context.Background()); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.ProviderEnabled("mock") {
		t.Fatal("ProviderEnabled(mock) = true after Load with provider.mock.enabled=false, want false")
	}

	// Simulate Postgres going unreachable mid-refresh.
	src.fail = true
	src.values["provider.mock.enabled"] = "true" // would flip the flag IF this refresh succeeded
	c.Refresh(context.Background())

	if c.ProviderEnabled("mock") {
		t.Error("ProviderEnabled(mock) = true after a FAILED refresh, want it to still report the last-known-good value (false) -- fallback did not hold")
	}

	// Bring "Postgres" back and confirm refresh resumes.
	src.fail = false
	c.Refresh(context.Background())
	if !c.ProviderEnabled("mock") {
		t.Error("ProviderEnabled(mock) = false after refresh resumed with provider.mock.enabled=true, want true -- refresh did not resume")
	}
}

// TestCache_Run_ContinuesTickingAfterARefreshFailure exercises the SAME
// fallback guarantee through the actual background-refresh actor (Run) that
// internal/platform/lifecycle.RunnerActor wraps for oklog/run.Group in cmd/worker --
// not just the one-shot Refresh helper -- proving a failed tick does not
// stop the loop (no panic, no early return) and a later tick still
// recovers.
func TestCache_Run_ContinuesTickingAfterARefreshFailure(t *testing.T) {
	src := &fakeConfigSource{values: map[string]string{"k": "v1"}, fail: true}
	c := NewCache(src, 5*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	// Let a few failing ticks happen -- Run must not exit or panic.
	deadlineFail := time.Now().Add(100 * time.Millisecond)
	for src.callCount() < 3 && time.Now().Before(deadlineFail) {
		time.Sleep(2 * time.Millisecond)
	}
	if src.callCount() < 1 {
		t.Fatal("Run did not attempt any refresh ticks")
	}
	if v, _ := c.Get("k"); v != "" {
		t.Errorf("Get(k) = %q after only-failing refreshes (cache started empty), want empty", v)
	}

	// Recover: subsequent ticks should succeed and populate the cache.
	src.setFail(false)
	deadlineOK := time.Now().Add(200 * time.Millisecond)
	for {
		if v, ok := c.Get("k"); ok && v == "v1" {
			break
		}
		if time.Now().After(deadlineOK) {
			t.Fatal("cache never picked up the value after refresh recovered")
		}
		time.Sleep(2 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned error %v after ctx cancellation, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return after ctx cancellation")
	}
}

func TestCache_GetBool_DefaultsWhenKeyAbsent(t *testing.T) {
	src := &fakeConfigSource{values: map[string]string{}}
	c := NewCache(src, time.Hour)
	if err := c.Load(context.Background()); err != nil {
		t.Fatalf("Load: %v", err)
	}

	if !c.ProviderEnabled("never-configured") {
		t.Error("ProviderEnabled for a provider with no app_config row = false, want true (default enabled)")
	}
}

func TestCache_GetBool_InvalidValueUsesDefault(t *testing.T) {
	src := &fakeConfigSource{values: map[string]string{"provider.mock.enabled": "not-a-bool"}}
	c := NewCache(src, time.Hour)
	if err := c.Load(context.Background()); err != nil {
		t.Fatalf("Load: %v", err)
	}

	if !c.GetBool("provider.mock.enabled", true) {
		t.Error("GetBool with an unparsable stored value did not fall back to the provided default")
	}
}

func TestCache_QueueName_UsesSeedDefaultWhenRowAbsent(t *testing.T) {
	src := &fakeConfigSource{values: map[string]string{}}
	c := NewCache(src, time.Hour)
	if err := c.Load(context.Background()); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := c.QueueName("my-seed-default"); got != "my-seed-default" {
		t.Errorf("QueueName with no app_config row = %q, want seed default %q", got, "my-seed-default")
	}
}

func TestCache_QueueName_RowOverridesSeedDefault(t *testing.T) {
	src := &fakeConfigSource{values: map[string]string{QueueDefaultNameKey: "payments"}}
	c := NewCache(src, time.Hour)
	if err := c.Load(context.Background()); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := c.QueueName("default"); got != "payments" {
		t.Errorf("QueueName = %q, want app_config row value %q to win over seed default", got, "payments")
	}
}

func TestCache_OtelEnabled_RowOverridesSeedDefault(t *testing.T) {
	src := &fakeConfigSource{values: map[string]string{ObservabilityOtelKey: "false"}}
	c := NewCache(src, time.Hour)
	if err := c.Load(context.Background()); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.OtelEnabled(true) {
		t.Error("OtelEnabled = true, want the app_config row (false) to override the seed default (true)")
	}
}

func TestCache_RateLimitEnabled_RowOverridesSeedDefault(t *testing.T) {
	src := &fakeConfigSource{values: map[string]string{RateLimitEnabledKey: "false"}}
	c := NewCache(src, time.Hour)
	if err := c.Load(context.Background()); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.RateLimitEnabled(true) {
		t.Error("RateLimitEnabled = true, want the app_config row (false) to override the seed default (true)")
	}
}

func TestCache_CircuitBreakerEnabled_RowOverridesSeedDefault(t *testing.T) {
	src := &fakeConfigSource{values: map[string]string{CircuitBreakerEnabledKey: "false"}}
	c := NewCache(src, time.Hour)
	if err := c.Load(context.Background()); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.CircuitBreakerEnabled(true) {
		t.Error("CircuitBreakerEnabled = true, want the app_config row (false) to override the seed default (true)")
	}
}

func TestCache_ReconciliationEnabled_RowOverridesSeedDefault(t *testing.T) {
	src := &fakeConfigSource{values: map[string]string{ReconciliationEnabledKey: "false"}}
	c := NewCache(src, time.Hour)
	if err := c.Load(context.Background()); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.ReconciliationEnabled(true) {
		t.Error("ReconciliationEnabled = true, want the app_config row (false) to override the seed default (true)")
	}
}

func TestLoadDynamicSeed_MissingFileUsesHardcodedDefaults(t *testing.T) {
	seed, err := LoadDynamicSeed("/nonexistent/config.toml")
	if err != nil {
		t.Fatalf("LoadDynamicSeed: %v", err)
	}
	want := DynamicSeed{
		QueueDefaultName:      DefaultQueueName,
		OtelEnabled:           DefaultObservabilityOn,
		RateLimitEnabled:      DefaultRateLimitEnabled,
		CircuitBreakerEnabled: DefaultCircuitBreakerOn,
		ReconciliationEnabled: DefaultReconciliationOn,
	}
	if seed != want {
		t.Errorf("LoadDynamicSeed on missing file = %+v, want hardcoded defaults %+v", seed, want)
	}
}

func TestLoadDynamicSeed_ReadsQueueAndObservabilitySections(t *testing.T) {
	path := writeTempTOML(t, `
[queue]
default_name = "payments"

[observability]
otel_enabled = false

[ratelimit]
enabled = false

[circuitbreaker]
enabled = false

[reconciliation]
enabled = false
`)

	seed, err := LoadDynamicSeed(path)
	if err != nil {
		t.Fatalf("LoadDynamicSeed: %v", err)
	}
	want := DynamicSeed{
		QueueDefaultName:      "payments",
		OtelEnabled:           false,
		RateLimitEnabled:      false,
		CircuitBreakerEnabled: false,
		ReconciliationEnabled: false,
	}
	if seed != want {
		t.Errorf("LoadDynamicSeed = %+v, want %+v (every section overridden by the file)", seed, want)
	}
}
