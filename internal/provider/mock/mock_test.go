package mock

import (
	"context"
	"errors"
	"testing"
	"time"

	"Fisher-Mapper/internal/provider"
)

func TestMock_ChargeSucceedsByDefault(t *testing.T) {
	m := New(Config{Name: "mock"})

	resp, err := m.Charge(context.Background(), provider.ChargeRequest{IdempotencyKey: "k1", Amount: 100, Currency: "USD"})
	if err != nil {
		t.Fatalf("Charge: %v", err)
	}
	if resp.Status != provider.StatusSucceeded {
		t.Errorf("Status = %q, want %q", resp.Status, provider.StatusSucceeded)
	}
	if resp.ProviderRef == "" {
		t.Error("ProviderRef is empty")
	}
	if m.CallCounts().Charge != 1 {
		t.Errorf("Charge call count = %d, want 1", m.CallCounts().Charge)
	}
}

func TestMock_ChargeSameIdempotencyKeyReturnsSameRef(t *testing.T) {
	m := New(Config{Name: "mock"})

	r1, err := m.Charge(context.Background(), provider.ChargeRequest{IdempotencyKey: "same-key"})
	if err != nil {
		t.Fatalf("Charge: %v", err)
	}
	r2, err := m.Charge(context.Background(), provider.ChargeRequest{IdempotencyKey: "same-key"})
	if err != nil {
		t.Fatalf("Charge: %v", err)
	}
	if r1.ProviderRef != r2.ProviderRef {
		t.Errorf("ProviderRef differs across calls with the same idempotency key: %q != %q", r1.ProviderRef, r2.ProviderRef)
	}
}

func TestMock_ForceErrorAppliesToAllMethods(t *testing.T) {
	forced := errors.New("boom")
	m := New(Config{Name: "mock", ForceError: forced})

	if _, err := m.Charge(context.Background(), provider.ChargeRequest{}); !errors.Is(err, forced) {
		t.Errorf("Charge error = %v, want %v", err, forced)
	}
	if _, err := m.Authorize(context.Background(), provider.AuthorizeRequest{}); !errors.Is(err, forced) {
		t.Errorf("Authorize error = %v, want %v", err, forced)
	}
	if _, err := m.GetStatus(context.Background(), provider.GetStatusRequest{}); !errors.Is(err, forced) {
		t.Errorf("GetStatus error = %v, want %v", err, forced)
	}
}

// TestMock_LatencyLongerThanContextDeadlineTimesOut simulates the
// "provider call timed out" case Phase 3 exercises for real: Latency is
// configured longer than the caller's context deadline, so Charge must
// return the context's deadline-exceeded error rather than blocking
// forever or silently succeeding.
func TestMock_LatencyLongerThanContextDeadlineTimesOut(t *testing.T) {
	m := New(Config{Name: "mock", Latency: 200 * time.Millisecond})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := m.Charge(ctx, provider.ChargeRequest{IdempotencyKey: "timeout-key"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Charge error = %v, want context.DeadlineExceeded", err)
	}
}
