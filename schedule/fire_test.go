package schedule

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// waitForCalls polls until the enqueuer has recorded at least n calls, or fails
// the test. Polling keeps the test fast when the fire is prompt and tolerant
// when the machine is loaded, which matters under `make test-race` (-count 10).
func waitForCalls(t *testing.T, enq *fakeEnqueuer, n int) []enqueued {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if calls := enq.snapshot(); len(calls) >= n {
			return calls
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected at least %d enqueue(s), got %d", n, len(enq.snapshot()))
	return nil
}

func TestFiringEnqueuesTheTask(t *testing.T) {
	ctx := context.Background()
	s, _, enq := newTestSchedulerInterval(t, 10*time.Millisecond)

	if _, err := s.Create(ctx, Schedule{
		TaskName: "scan",
		Cron:     "0 2 * * *", // ignored by the test trigger, but still validated
		Params:   json.RawMessage(`{"full":true}`),
		Enabled:  true,
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	calls := waitForCalls(t, enq, 1)
	if calls[0].name != "scan" {
		t.Errorf("expected task name \"scan\", got %q", calls[0].name)
	}
	if string(calls[0].params) != `{"full":true}` {
		t.Errorf("expected the schedule's params, got %s", calls[0].params)
	}
}

func TestFiringRepeats(t *testing.T) {
	ctx := context.Background()
	s, _, enq := newTestSchedulerInterval(t, 10*time.Millisecond)

	if _, err := s.Create(ctx, Schedule{TaskName: "scan", Cron: "0 2 * * *", Enabled: true}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	waitForCalls(t, enq, 3)
}

func TestFiringSurvivesAFullQueue(t *testing.T) {
	ctx := context.Background()
	s, _, enq := newTestSchedulerInterval(t, 10*time.Millisecond)
	enq.setErr(errors.New("queue full"))

	created, err := s.Create(ctx, Schedule{TaskName: "scan", Cron: "0 2 * * *", Enabled: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Give the trigger several chances to fire and fail.
	time.Sleep(100 * time.Millisecond)

	if !hasJob(t, s, created.ID) {
		t.Error("expected a failed enqueue to leave the job scheduled")
	}
	// Once the queue drains, firing resumes.
	enq.setErr(nil)
	waitForCalls(t, enq, 1)
}

func TestDisabledSchedulesNeverFire(t *testing.T) {
	ctx := context.Background()
	s, _, enq := newTestSchedulerInterval(t, 10*time.Millisecond)

	created, err := s.Create(ctx, Schedule{TaskName: "scan", Cron: "0 2 * * *", Enabled: false})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if calls := enq.snapshot(); len(calls) != 0 {
		t.Fatalf("expected no fires for a disabled schedule, got %d", len(calls))
	}

	if _, err := s.SetEnabled(ctx, created.ID, true); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	waitForCalls(t, enq, 1)
}

func TestDeletedSchedulesStopFiring(t *testing.T) {
	ctx := context.Background()
	s, _, enq := newTestSchedulerInterval(t, 10*time.Millisecond)

	created, err := s.Create(ctx, Schedule{TaskName: "scan", Cron: "0 2 * * *", Enabled: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	waitForCalls(t, enq, 1)

	if err := s.Delete(ctx, created.ID); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	before := len(enq.snapshot())
	time.Sleep(100 * time.Millisecond)
	// Allow for one fire that was already in flight when Delete ran.
	if after := len(enq.snapshot()); after > before+1 {
		t.Errorf("expected firing to stop after Delete: %d -> %d", before, after)
	}
}

func TestParamsAreNotSharedBetweenFires(t *testing.T) {
	ctx := context.Background()
	s, _, enq := newTestSchedulerInterval(t, 10*time.Millisecond)

	if _, err := s.Create(ctx, Schedule{
		TaskName: "scan",
		Cron:     "0 2 * * *",
		Params:   json.RawMessage(`{"full":true}`),
		Enabled:  true,
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	first := waitForCalls(t, enq, 1)[0]
	// Simulate a raw handler mutating the slice it received.
	first.params[2] = 'X'

	calls := waitForCalls(t, enq, 2)
	if string(calls[1].params) != `{"full":true}` {
		t.Errorf("a later fire saw mutated params: %s", calls[1].params)
	}
}

func TestNextFireAtAdvances(t *testing.T) {
	ctx := context.Background()
	s, _, _ := newTestSchedulerInterval(t, 50*time.Millisecond)

	created, err := s.Create(ctx, Schedule{TaskName: "scan", Cron: "0 2 * * *", Enabled: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if created.NextFireAt.IsZero() {
		t.Fatal("expected a next fire time")
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got, err := s.Get(ctx, created.ID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.NextFireAt.After(created.NextFireAt) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Error("expected NextFireAt to advance after a fire")
}
