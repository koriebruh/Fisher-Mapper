package observability

import (
	"context"
	"testing"
)

func TestNewTracerManager_DisabledInstallsNoopAndNilErrShutdown(t *testing.T) {
	tm := NewTracerManager(context.Background(), "test-svc", false)
	if tm.enabled {
		t.Error("enabled = true after NewTracerManager(false), want false")
	}
	if err := tm.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown on a disabled (noop) manager returned %v, want nil", err)
	}
}

func TestNewTracerManager_EnabledInstallsRealProvider(t *testing.T) {
	tm := NewTracerManager(context.Background(), "test-svc", true)
	if !tm.enabled {
		t.Error("enabled = false after NewTracerManager(true), want true")
	}
	if err := tm.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown on an enabled manager returned %v, want nil", err)
	}
}

func TestTracerManager_Reconcile_NoopWhenSettingUnchanged(t *testing.T) {
	tm := NewTracerManager(context.Background(), "test-svc", true)

	tm.Reconcile(context.Background(), true)

	if !tm.enabled {
		t.Error("enabled flipped to false after Reconcile(true) on an already-enabled manager")
	}
}

func TestTracerManager_Reconcile_SwapsWhenSettingDiffers(t *testing.T) {
	tm := NewTracerManager(context.Background(), "test-svc", true)
	if !tm.enabled {
		t.Fatal("expected enabled=true after construction")
	}

	tm.Reconcile(context.Background(), false)
	if tm.enabled {
		t.Error("enabled = true after Reconcile(false), want false (noop provider swapped in)")
	}

	tm.Reconcile(context.Background(), true)
	if !tm.enabled {
		t.Error("enabled = false after Reconcile(true), want true (real provider swapped back in)")
	}
}
