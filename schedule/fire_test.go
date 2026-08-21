package schedule

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/reugn/go-quartz/quartz"
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

// TestStartsContextDoesNotStopTheScheduler pins the lifetime rule: go-quartz
// stops firing as soon as the context it was started with is done, so Start must
// not hand it the caller's ctx. Otherwise the everyday
//
//	ctx, cancel := context.WithTimeout(...); defer cancel(); sched.Start(ctx)
//
// wiring kills the scheduler on the way out of the wiring function, silently.
func TestStartsContextDoesNotStopTheScheduler(t *testing.T) {
	st := NewMemStore()
	enq := &fakeEnqueuer{}
	s, err := New(Cfg{Store: st, Enqueuer: enq, Logger: quietLogger()})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	s.newTrigger = func(_ string) (quartz.Trigger, error) {
		return quartz.NewSimpleTrigger(10 * time.Millisecond), nil
	}

	startCtx, cancelStart := context.WithCancel(context.Background())
	if err := s.Start(startCtx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.ShutDown(ctx)
	})

	ctx := context.Background()
	if _, err := s.Create(ctx, Schedule{TaskName: "scan", Cron: "0 2 * * *", Enabled: true}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	before := len(waitForCalls(t, enq, 1))

	// The caller is done with its start context and drops it, as it should be
	// free to. Firing must carry on: only ShutDown ends the Scheduler.
	cancelStart()
	waitForCalls(t, enq, before+2)

	// And nothing has shut down, so Wait must still block.
	done := make(chan struct{})
	go func() { s.Wait(); close(done) }()
	select {
	case <-done:
		t.Error("Wait returned although the Scheduler was never shut down")
	case <-time.After(50 * time.Millisecond):
	}
}

// TestCreateTakesItsOwnCopyOfParams pins that Create does not alias the caller's
// slice. Without the copy this is a data race against the quartz worker, and it
// corrupts the fired params, the stored params and what Get reports.
func TestCreateTakesItsOwnCopyOfParams(t *testing.T) {
	ctx := context.Background()
	s, st, enq := newTestSchedulerInterval(t, 10*time.Millisecond)

	// The caller keeps ownership of its buffer and reuses it, as documented.
	params := []byte(`{"full":true}`)
	created, err := s.Create(ctx, Schedule{
		TaskName: "scan",
		Cron:     "0 2 * * *",
		Params:   params,
		Enabled:  true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Keep writing to it while the schedule fires, so the race detector sees the
	// overlap rather than depending on the timing of a single write.
	stop := make(chan struct{})
	mutated := make(chan struct{})
	go func() {
		defer close(mutated)
		for {
			select {
			case <-stop:
				return
			default:
				copy(params, `{"full":9999}`)
				time.Sleep(100 * time.Microsecond)
			}
		}
	}()

	calls := waitForCalls(t, enq, 2)
	close(stop)
	<-mutated

	for i, c := range calls {
		if string(c.params) != `{"full":true}` {
			t.Errorf("fire %d saw the caller's mutation: %s", i, c.params)
		}
	}
	if string(created.Params) != `{"full":true}` {
		t.Errorf("the returned schedule saw the caller's mutation: %s", created.Params)
	}
	stored, err := st.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(stored.Params) != `{"full":true}` {
		t.Errorf("the stored params saw the caller's mutation: %s", stored.Params)
	}
	got, err := s.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(got.Params) != `{"full":true}` {
		t.Errorf("Get returned mutated params: %s", got.Params)
	}
}

// TestUpdateTakesItsOwnCopyOfParams is the Update half of the rule above.
func TestUpdateTakesItsOwnCopyOfParams(t *testing.T) {
	ctx := context.Background()
	s, st, _ := newTestScheduler(t)

	created, err := s.Create(ctx, Schedule{TaskName: "scan", Cron: "0 2 * * *", Enabled: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	params := []byte(`{"full":true}`)
	updated := created.Schedule
	updated.Params = params
	got, err := s.Update(ctx, updated)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	copy(params, `{"full":9999}`)

	if string(got.Params) != `{"full":true}` {
		t.Errorf("the returned schedule saw the caller's mutation: %s", got.Params)
	}
	stored, err := st.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(stored.Params) != `{"full":true}` {
		t.Errorf("the stored params saw the caller's mutation: %s", stored.Params)
	}
}

func TestNextFireAtAdvances(t *testing.T) {
	ctx := context.Background()
	s, _, _ := newTestSchedulerInterval(t, 50*time.Millisecond)

	created, err := s.Create(ctx, Schedule{TaskName: "scan", Cron: "0 2 * * *", Enabled: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if created.NextFireAt == nil {
		t.Fatal("expected a next fire time")
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got, err := s.Get(ctx, created.ID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.NextFireAt != nil && got.NextFireAt.After(*created.NextFireAt) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Error("expected NextFireAt to advance after a fire")
}
