package outbox

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"Fisher-Mapper/internal/platform/queue"
)

// fakeQueueClient records the EnqueueOptions it was called with, so tests
// can assert on QueueName/MaxRetry without a live asynq/Redis connection.
type fakeQueueClient struct {
	lastOpts queue.EnqueueOptions
}

func (f *fakeQueueClient) Enqueue(_ context.Context, _ string, _ []byte, opts queue.EnqueueOptions) error {
	f.lastOpts = opts
	return nil
}

func (f *fakeQueueClient) Close() error { return nil }

var _ queue.Client = (*fakeQueueClient)(nil)

func TestRelay_DispatchOne_UsesConfiguredQueueName(t *testing.T) {
	client := &fakeQueueClient{}
	relay := NewRelay(nil, client, 0, 0, 0).WithQueueName(func() string { return "payments" })

	row := Row{ID: uuid.New(), TaskType: "some-task", Payload: []byte(`{}`)}
	if err := relay.dispatchOne(context.Background(), row); err != nil {
		t.Fatalf("dispatchOne: %v", err)
	}
	if client.lastOpts.QueueName != "payments" {
		t.Errorf("QueueName = %q, want %q", client.lastOpts.QueueName, "payments")
	}
}

func TestRelay_DispatchOne_NoQueueNameGetterLeavesOptionsEmpty(t *testing.T) {
	client := &fakeQueueClient{}
	relay := NewRelay(nil, client, 0, 0, 0)

	row := Row{ID: uuid.New(), TaskType: "some-task", Payload: []byte(`{}`)}
	if err := relay.dispatchOne(context.Background(), row); err != nil {
		t.Fatalf("dispatchOne: %v", err)
	}
	if client.lastOpts.QueueName != "" {
		t.Errorf("QueueName = %q, want empty (asynq default queue) when WithQueueName was never called", client.lastOpts.QueueName)
	}
}
