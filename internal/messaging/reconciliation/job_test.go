package reconciliation

import (
	"context"
	"testing"
	"time"

	"Fisher-Mapper/internal/domain/payment"
)

// fakeService counts calls instead of touching Postgres, so the
// reconciliation.enabled toggle's short-circuit can be proven without a live
// DB connection (mirrors internal/platform/config's fakeConfigSource for the
// same reason).
type fakeService struct {
	listCalls  int
	sweepCalls int
}

func (f *fakeService) ListStuckProcessing(context.Context, time.Duration) ([]*payment.Payment, error) {
	f.listCalls++
	return nil, nil
}

func (f *fakeService) ReconcilePayment(context.Context, *payment.Payment) error {
	return nil
}

func (f *fakeService) SweepStagedWebhooks(context.Context) (int, error) {
	f.sweepCalls++
	return 0, nil
}

// TestJob_RunOnce_EnabledNilRunsAsNormal proves the backward-compatible
// default (no WithEnabledCheck call at all, same as before the toggle
// existed): the job does its work.
func TestJob_RunOnce_EnabledNilRunsAsNormal(t *testing.T) {
	fake := &fakeService{}
	j := &Job{service: fake, threshold: time.Minute}

	j.RunOnce(context.Background())

	if fake.listCalls != 1 || fake.sweepCalls != 1 {
		t.Errorf("listCalls=%d sweepCalls=%d, want 1 and 1 (enabled defaults to true when unset)", fake.listCalls, fake.sweepCalls)
	}
}

// TestJob_RunOnce_EnabledFalseSkipsWork proves the toggle actually changes
// behavior: with the SAME fake and threshold as above, enabled=false must
// skip both the stuck-payment poll and the staged-webhook sweep entirely.
func TestJob_RunOnce_EnabledFalseSkipsWork(t *testing.T) {
	fake := &fakeService{}
	j := (&Job{service: fake, threshold: time.Minute}).WithEnabledCheck(func() bool { return false })

	j.RunOnce(context.Background())

	if fake.listCalls != 0 || fake.sweepCalls != 0 {
		t.Errorf("listCalls=%d sweepCalls=%d, want 0 and 0 (reconciliation disabled via dynamic config)", fake.listCalls, fake.sweepCalls)
	}
}

// TestJob_RunOnce_EnabledTrueRunsAsNormal proves an explicit enabled=true
// behaves identically to the nil default.
func TestJob_RunOnce_EnabledTrueRunsAsNormal(t *testing.T) {
	fake := &fakeService{}
	j := (&Job{service: fake, threshold: time.Minute}).WithEnabledCheck(func() bool { return true })

	j.RunOnce(context.Background())

	if fake.listCalls != 1 || fake.sweepCalls != 1 {
		t.Errorf("listCalls=%d sweepCalls=%d, want 1 and 1", fake.listCalls, fake.sweepCalls)
	}
}
