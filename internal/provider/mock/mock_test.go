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

// TestMock_ChargeMethodPayloadShapePerPaymentMethod asserts Charge populates
// exactly the payload variant matching PaymentMethod (all others nil), and
// nil MethodPayload entirely for anything else -- the shape a real REST/gRPC
// GetPayment response is expected to expose verbatim (Step 3+).
func TestMock_ChargeMethodPayloadShapePerPaymentMethod(t *testing.T) {
	m := New(Config{Name: "mock"})

	t.Run("qris", func(t *testing.T) {
		resp, err := m.Charge(context.Background(), provider.ChargeRequest{IdempotencyKey: "qris-key", PaymentMethod: "qris"})
		if err != nil {
			t.Fatalf("Charge: %v", err)
		}
		mp := resp.MethodPayload
		if mp == nil || mp.QRIS == nil {
			t.Fatalf("MethodPayload.QRIS = nil, want set")
		}
		if mp.VirtualAccount != nil || mp.Card != nil || mp.EWallet != nil {
			t.Errorf("non-QRIS variants must stay nil, got %+v", mp)
		}
		if mp.QRIS.QRString == nil || *mp.QRIS.QRString == "" {
			t.Error("QRIS.QRString must be a non-empty pointer")
		}
		if mp.QRIS.ExpiresAt == nil {
			t.Error("QRIS.ExpiresAt must be set")
		}
	})

	t.Run("virtual_account", func(t *testing.T) {
		resp, err := m.Charge(context.Background(), provider.ChargeRequest{IdempotencyKey: "va-key", PaymentMethod: "virtual_account"})
		if err != nil {
			t.Fatalf("Charge: %v", err)
		}
		mp := resp.MethodPayload
		if mp == nil || mp.VirtualAccount == nil {
			t.Fatalf("MethodPayload.VirtualAccount = nil, want set")
		}
		if mp.QRIS != nil || mp.Card != nil || mp.EWallet != nil {
			t.Errorf("non-VA variants must stay nil, got %+v", mp)
		}
		for _, r := range mp.VirtualAccount.VANumber {
			if r < '0' || r > '9' {
				t.Fatalf("VANumber must be all digits, got %q", mp.VirtualAccount.VANumber)
			}
		}
	})

	t.Run("card", func(t *testing.T) {
		resp, err := m.Charge(context.Background(), provider.ChargeRequest{IdempotencyKey: "card-key", PaymentMethod: "card"})
		if err != nil {
			t.Fatalf("Charge: %v", err)
		}
		mp := resp.MethodPayload
		if mp == nil || mp.Card == nil {
			t.Fatalf("MethodPayload.Card = nil, want set")
		}
		if mp.QRIS != nil || mp.VirtualAccount != nil || mp.EWallet != nil {
			t.Errorf("non-Card variants must stay nil, got %+v", mp)
		}
		if mp.Card.MaskedPAN == nil || *mp.Card.MaskedPAN == "" {
			t.Error("Card.MaskedPAN must be a non-empty pointer")
		}
	})

	t.Run("unrecognized method -> nil payload", func(t *testing.T) {
		resp, err := m.Charge(context.Background(), provider.ChargeRequest{IdempotencyKey: "other-key", PaymentMethod: "bank_transfer"})
		if err != nil {
			t.Fatalf("Charge: %v", err)
		}
		if resp.MethodPayload != nil {
			t.Errorf("MethodPayload = %+v, want nil for an unrecognized payment method", resp.MethodPayload)
		}
	})
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
