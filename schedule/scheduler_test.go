package schedule

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/reugn/go-quartz/quartz"
)

// newTestScheduler returns a started Scheduler over a MemStore, plus the store
// and the fake enqueuer, and registers cleanup. Triggers are replaced with a
// SimpleTrigger so tests never wait for cron's one-second granularity. The
// interval is long enough that nothing fires unless a test wants it to.
func newTestScheduler(t *testing.T) (*Scheduler, *MemStore, *fakeEnqueuer) {
	t.Helper()
	return newTestSchedulerInterval(t, time.Hour)
}

func newTestSchedulerInterval(t *testing.T, every time.Duration) (*Scheduler, *MemStore, *fakeEnqueuer) {
	t.Helper()
	st := NewMemStore()
	enq := &fakeEnqueuer{}
	s, err := New(Cfg{Store: st, Enqueuer: enq, Logger: quietLogger()})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	s.newTrigger = func(_ string) (quartz.Trigger, error) {
		return quartz.NewSimpleTrigger(every), nil
	}
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.ShutDown(ctx)
	})
	return s, st, enq
}

// jobKeys returns the string keys of every job currently registered.
func jobKeys(t *testing.T, s *Scheduler) []string {
	t.Helper()
	keys, err := s.qs.GetJobKeys()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, k.String())
	}
	return out
}

func hasJob(t *testing.T, s *Scheduler, id uuid.UUID) bool {
	t.Helper()
	for _, k := range jobKeys(t, s) {
		if k == jobKey(id).String() {
			return true
		}
	}
	return false
}

func TestNew(t *testing.T) {
	t.Run("requires a store", func(t *testing.T) {
		if _, err := New(Cfg{Enqueuer: &fakeEnqueuer{}}); err == nil {
			t.Error("expected an error when Store is nil")
		}
	})
	t.Run("requires an enqueuer", func(t *testing.T) {
		if _, err := New(Cfg{Store: NewMemStore()}); err == nil {
			t.Error("expected an error when Enqueuer is nil")
		}
	})
	t.Run("defaults the logger and location", func(t *testing.T) {
		s, err := New(Cfg{Store: NewMemStore(), Enqueuer: &fakeEnqueuer{}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if s.log == nil {
			t.Error("expected a default logger")
		}
		if s.newTrigger == nil {
			t.Error("expected a default trigger builder")
		}
	})
}

// TestLocationIsHonoured exercises the real (non-substituted) trigger builder,
// so it is the one test that proves Cfg.Location reaches go-quartz.
func TestLocationIsHonoured(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name     string
		loc      *time.Location
		wantHour int // hour of NextFireAt expressed in UTC
	}{
		{name: "utc", loc: time.UTC, wantHour: 2},
		{name: "five hours east", loc: time.FixedZone("east", 5*60*60), wantHour: 21},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st := NewMemStore()
			s, err := New(Cfg{
				Store:    st,
				Enqueuer: &fakeEnqueuer{},
				Logger:   quietLogger(),
				Location: tc.loc,
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if err := s.Start(ctx); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			t.Cleanup(func() { _ = s.ShutDown(ctx) })

			// 02:00 every day, in the configured zone. Written straight to the
			// store and loaded with Reload, since Create arrives in Task 5.
			sch := Schedule{ID: uuid.New(), TaskName: "scan", Cron: "0 0 2 * * *", Enabled: true, CreatedAt: time.Unix(1, 0)}
			if err := st.Save(ctx, sch); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if err := s.Reload(ctx); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			got, err := s.Get(ctx, sch.ID)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.NextFireAt.IsZero() {
				t.Fatal("expected a next fire time")
			}
			if h := got.NextFireAt.UTC().Hour(); h != tc.wantHour {
				t.Errorf("expected the next fire at %02d:00 UTC, got %v", tc.wantHour, got.NextFireAt.UTC())
			}
		})
	}
}

// TestUnixSundayTranslationFiresOnSunday proves that the Unix form "0 3 * * 0"
// (Sunday in Unix cron) translates to Quartz and fires on Sunday, not Saturday.
func TestUnixSundayTranslationFiresOnSunday(t *testing.T) {
	ctx := context.Background()
	st := NewMemStore()
	s, err := New(Cfg{
		Store:    st,
		Enqueuer: &fakeEnqueuer{},
		Logger:   quietLogger(),
		Location: time.UTC,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := s.Start(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Cleanup(func() { _ = s.ShutDown(ctx) })

	sch := Schedule{ID: uuid.New(), TaskName: "scan", Cron: "0 3 * * 0", Enabled: true, CreatedAt: time.Unix(1, 0)}
	if err := st.Save(ctx, sch); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := s.Reload(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got, err := s.Get(ctx, sch.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.NextFireAt.IsZero() {
		t.Fatal("expected a next fire time")
	}
	if got.NextFireAt.UTC().Weekday() != time.Sunday {
		t.Errorf("expected next fire on Sunday, got %v", got.NextFireAt.UTC().Weekday())
	}
}

func TestStart(t *testing.T) {
	ctx := context.Background()

	t.Run("loads enabled schedules and skips disabled ones", func(t *testing.T) {
		st := NewMemStore()
		on := Schedule{ID: uuid.New(), TaskName: "scan", Cron: "0 0 2 * * *", Enabled: true, CreatedAt: time.Unix(1, 0)}
		off := Schedule{ID: uuid.New(), TaskName: "scan", Cron: "0 0 3 * * *", Enabled: false, CreatedAt: time.Unix(2, 0)}
		_ = st.Save(ctx, on)
		_ = st.Save(ctx, off)

		s, err := New(Cfg{Store: st, Enqueuer: &fakeEnqueuer{}, Logger: quietLogger()})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := s.Start(ctx); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		t.Cleanup(func() { _ = s.ShutDown(ctx) })

		if !hasJob(t, s, on.ID) {
			t.Error("expected the enabled schedule to be registered")
		}
		if hasJob(t, s, off.ID) {
			t.Error("expected the disabled schedule not to be registered")
		}
	})

	t.Run("fails when the store cannot be read", func(t *testing.T) {
		wantErr := errors.New("db down")
		s, err := New(Cfg{Store: &failingStore{listErr: wantErr}, Enqueuer: &fakeEnqueuer{}, Logger: quietLogger()})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		t.Cleanup(func() { _ = s.ShutDown(ctx) })
		if err := s.Start(ctx); !errors.Is(err, wantErr) {
			t.Errorf("expected the store error, got %v", err)
		}
	})

	t.Run("skips rows whose cron no longer parses", func(t *testing.T) {
		st := NewMemStore()
		good := Schedule{ID: uuid.New(), TaskName: "scan", Cron: "0 0 2 * * *", Enabled: true, CreatedAt: time.Unix(1, 0)}
		bad := Schedule{ID: uuid.New(), TaskName: "scan", Cron: "not a cron", Enabled: true, CreatedAt: time.Unix(2, 0)}
		_ = st.Save(ctx, good)
		_ = st.Save(ctx, bad)

		s, err := New(Cfg{Store: st, Enqueuer: &fakeEnqueuer{}, Logger: quietLogger()})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := s.Start(ctx); err != nil {
			t.Fatalf("expected a bad row to be skipped, not to fail Start: %v", err)
		}
		t.Cleanup(func() { _ = s.ShutDown(ctx) })

		if !hasJob(t, s, good.ID) {
			t.Error("expected the valid schedule to be registered")
		}
		if hasJob(t, s, bad.ID) {
			t.Error("expected the invalid schedule to be skipped")
		}
	})

	t.Run("is idempotent", func(t *testing.T) {
		s, _, _ := newTestScheduler(t)
		if err := s.Start(ctx); err != nil {
			t.Errorf("expected a second Start to be a no-op, got %v", err)
		}
	})
}

func TestReload(t *testing.T) {
	ctx := context.Background()

	t.Run("picks up out-of-band writes and removals", func(t *testing.T) {
		s, st, _ := newTestScheduler(t)
		added := Schedule{ID: uuid.New(), TaskName: "scan", Cron: "0 0 2 * * *", Enabled: true, CreatedAt: time.Unix(1, 0)}
		_ = st.Save(ctx, added)

		if hasJob(t, s, added.ID) {
			t.Fatal("a direct store write should not register a job on its own")
		}
		if err := s.Reload(ctx); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !hasJob(t, s, added.ID) {
			t.Error("expected Reload to register the new schedule")
		}

		_ = st.Delete(ctx, added.ID)
		if err := s.Reload(ctx); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if hasJob(t, s, added.ID) {
			t.Error("expected Reload to drop the removed schedule")
		}
	})

	t.Run("returns the store error", func(t *testing.T) {
		wantErr := errors.New("db down")
		s, err := New(Cfg{Store: &failingStore{listErr: wantErr}, Enqueuer: &fakeEnqueuer{}, Logger: quietLogger()})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := s.Reload(ctx); !errors.Is(err, wantErr) {
			t.Errorf("expected the store error, got %v", err)
		}
	})
}

func TestReadsReportNextFireAt(t *testing.T) {
	ctx := context.Background()
	s, st, _ := newTestScheduler(t)

	on := Schedule{ID: uuid.New(), TaskName: "scan", Cron: "0 0 2 * * *", Params: json.RawMessage(`{"full":true}`), Enabled: true, CreatedAt: time.Unix(1, 0)}
	off := Schedule{ID: uuid.New(), TaskName: "scan", Cron: "0 0 3 * * *", Enabled: false, CreatedAt: time.Unix(2, 0)}
	_ = st.Save(ctx, on)
	_ = st.Save(ctx, off)
	if err := s.Reload(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Run("list includes disabled schedules with a zero NextFireAt", func(t *testing.T) {
		list, err := s.List(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(list) != 2 {
			t.Fatalf("expected 2 schedules, got %d", len(list))
		}
		if list[0].ID != on.ID || list[1].ID != off.ID {
			t.Fatalf("expected store order to be preserved, got %v then %v", list[0].ID, list[1].ID)
		}
		if list[0].NextFireAt.IsZero() {
			t.Error("expected the enabled schedule to report a next fire time")
		}
		if !list[1].NextFireAt.IsZero() {
			t.Error("expected the disabled schedule to report a zero next fire time")
		}
		if string(list[0].Params) != `{"full":true}` {
			t.Errorf("expected params to survive the read, got %s", list[0].Params)
		}
	})

	t.Run("get returns one schedule", func(t *testing.T) {
		got, err := s.Get(ctx, on.ID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.ID != on.ID {
			t.Errorf("expected schedule %v, got %v", on.ID, got.ID)
		}
		if got.NextFireAt.IsZero() {
			t.Error("expected a next fire time")
		}
	})

	t.Run("get reports a missing schedule", func(t *testing.T) {
		if _, err := s.Get(ctx, uuid.New()); !errors.Is(err, ErrScheduleNotFound) {
			t.Errorf("expected ErrScheduleNotFound, got %v", err)
		}
	})
}

func TestShutDown(t *testing.T) {
	t.Run("wait returns once shut down", func(t *testing.T) {
		st := NewMemStore()
		s, err := New(Cfg{Store: st, Enqueuer: &fakeEnqueuer{}, Logger: quietLogger()})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		ctx := context.Background()
		if err := s.Start(ctx); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := s.ShutDown(ctx); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		done := make(chan struct{})
		go func() { s.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("Wait did not return after ShutDown")
		}
	})

	t.Run("is safe to call twice", func(t *testing.T) {
		s, _, _ := newTestScheduler(t)
		ctx := context.Background()
		if err := s.ShutDown(ctx); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := s.ShutDown(ctx); err != nil {
			t.Errorf("expected a second ShutDown to be a no-op, got %v", err)
		}
	})
}

// failingStore is a Store whose List always fails, to test how the Scheduler
// reacts to a store that is down. Only List is exercised; the other methods
// exist to satisfy the interface.
type failingStore struct{ listErr error }

func (f *failingStore) List(context.Context) ([]Schedule, error) {
	return nil, f.listErr
}

func (f *failingStore) Get(context.Context, uuid.UUID) (Schedule, error) {
	return Schedule{}, ErrScheduleNotFound
}

func (f *failingStore) Save(context.Context, Schedule) error {
	return nil
}

func (f *failingStore) Delete(context.Context, uuid.UUID) error {
	return ErrScheduleNotFound
}

// storeWithFailingDelete wraps a MemStore but makes Delete fail with a custom error.
type storeWithFailingDelete struct {
	*MemStore
	deleteErr error
}

func (s *storeWithFailingDelete) Delete(context.Context, uuid.UUID) error {
	return s.deleteErr
}

func TestCreate(t *testing.T) {
	ctx := context.Background()

	t.Run("persists and registers in one call", func(t *testing.T) {
		s, st, _ := newTestScheduler(t)
		got, err := s.Create(ctx, Schedule{
			TaskName: "scan",
			Cron:     "0 2 * * *",
			Params:   json.RawMessage(`{"full":true}`),
			Enabled:  true,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.ID == uuid.Nil {
			t.Error("expected an id to be generated")
		}
		if got.Cron != "0 0 2 * * *" {
			t.Errorf("expected the cron to be normalized, got %q", got.Cron)
		}
		if got.CreatedAt.IsZero() || got.UpdatedAt.IsZero() {
			t.Error("expected timestamps to be set")
		}
		if got.NextFireAt.IsZero() {
			t.Error("expected a next fire time for an enabled schedule")
		}
		if !hasJob(t, s, got.ID) {
			t.Error("expected the job to be registered")
		}
		stored, err := st.Get(ctx, got.ID)
		if err != nil {
			t.Fatalf("expected the schedule to be persisted: %v", err)
		}
		if string(stored.Params) != `{"full":true}` {
			t.Errorf("expected params to be persisted, got %s", stored.Params)
		}
	})

	t.Run("honours a caller-supplied id so a restore can keep it", func(t *testing.T) {
		s, _, _ := newTestScheduler(t)
		id := uuid.New()
		got, err := s.Create(ctx, Schedule{ID: id, TaskName: "scan", Cron: "0 2 * * *", Enabled: true})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.ID != id {
			t.Errorf("expected id %v, got %v", id, got.ID)
		}
	})

	t.Run("a disabled schedule is persisted but not registered", func(t *testing.T) {
		s, st, _ := newTestScheduler(t)
		got, err := s.Create(ctx, Schedule{TaskName: "scan", Cron: "0 2 * * *", Enabled: false})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if hasJob(t, s, got.ID) {
			t.Error("expected no job for a disabled schedule")
		}
		if !got.NextFireAt.IsZero() {
			t.Error("expected a zero next fire time")
		}
		if _, err := st.Get(ctx, got.ID); err != nil {
			t.Errorf("expected the schedule to be persisted: %v", err)
		}
	})

	t.Run("rejects an invalid cron before touching the store", func(t *testing.T) {
		s, st, _ := newTestScheduler(t)
		if _, err := s.Create(ctx, Schedule{TaskName: "scan", Cron: "not a cron", Enabled: true}); err == nil {
			t.Fatal("expected an error for an invalid cron")
		}
		list, _ := st.List(ctx)
		if len(list) != 0 {
			t.Errorf("expected nothing to be persisted, got %d schedules", len(list))
		}
	})

	t.Run("rejects a missing task name", func(t *testing.T) {
		s, _, _ := newTestScheduler(t)
		if _, err := s.Create(ctx, Schedule{Cron: "0 2 * * *", Enabled: true}); err == nil {
			t.Error("expected an error for an empty task name")
		}
	})

}

func TestCreateRejectsAnIdThatAlreadyExists(t *testing.T) {
	ctx := context.Background()
	s, st, _ := newTestScheduler(t)
	first, err := s.Create(ctx, Schedule{TaskName: "scan", Cron: "0 2 * * *", Enabled: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := s.Create(ctx, Schedule{ID: first.ID, TaskName: "other", Cron: "0 3 * * *", Enabled: true}); !errors.Is(err, ErrScheduleExists) {
		t.Errorf("expected ErrScheduleExists, got %v", err)
	}
	stored, _ := st.Get(ctx, first.ID)
	if stored.Cron != first.Cron {
		t.Errorf("expected the original cron to be untouched, got %q", stored.Cron)
	}
	if !stored.CreatedAt.Equal(first.CreatedAt) {
		t.Error("expected CreatedAt to be preserved")
	}
}

func TestCreateUndoesTheSaveWhenRegistrationFails(t *testing.T) {
	ctx := context.Background()
	st := NewMemStore()
	s, err := New(Cfg{Store: st, Enqueuer: &fakeEnqueuer{}, Logger: quietLogger()})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := s.Start(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Cleanup(func() { _ = s.ShutDown(ctx) })
	// A trigger builder that fails makes registration fail after the save.
	s.newTrigger = func(_ string) (quartz.Trigger, error) {
		return nil, errors.New("trigger boom")
	}

	if _, err := s.Create(ctx, Schedule{TaskName: "scan", Cron: "0 2 * * *", Enabled: true}); err == nil {
		t.Fatal("expected the create to fail")
	}
	list, _ := st.List(ctx)
	if len(list) != 0 {
		t.Errorf("expected the save to be undone, got %d schedules", len(list))
	}
}

func TestWritesBeforeStartAreRejected(t *testing.T) {
	ctx := context.Background()
	s, err := New(Cfg{Store: NewMemStore(), Enqueuer: &fakeEnqueuer{}, Logger: quietLogger()})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, err := s.Create(ctx, Schedule{TaskName: "scan", Cron: "0 2 * * *"}); !errors.Is(err, ErrNotStarted) {
		t.Errorf("Create: expected ErrNotStarted, got %v", err)
	}
	if _, err := s.Update(ctx, Schedule{ID: uuid.New(), TaskName: "scan", Cron: "0 2 * * *"}); !errors.Is(err, ErrNotStarted) {
		t.Errorf("Update: expected ErrNotStarted, got %v", err)
	}
	if _, err := s.SetEnabled(ctx, uuid.New(), true); !errors.Is(err, ErrNotStarted) {
		t.Errorf("SetEnabled: expected ErrNotStarted, got %v", err)
	}
	if err := s.Delete(ctx, uuid.New()); !errors.Is(err, ErrNotStarted) {
		t.Errorf("Delete: expected ErrNotStarted, got %v", err)
	}
}

func TestUpdate(t *testing.T) {
	ctx := context.Background()

	t.Run("replaces cron and params and re-registers", func(t *testing.T) {
		s, st, _ := newTestScheduler(t)
		created, err := s.Create(ctx, Schedule{TaskName: "scan", Cron: "0 2 * * *", Params: json.RawMessage(`{"full":false}`), Enabled: true})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		updated := created.Schedule
		updated.Cron = "0 5 * * *"
		updated.Params = json.RawMessage(`{"full":true}`)
		got, err := s.Update(ctx, updated)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.Cron != "0 0 5 * * *" {
			t.Errorf("expected the new normalized cron, got %q", got.Cron)
		}
		if !got.CreatedAt.Equal(created.CreatedAt) {
			t.Error("expected CreatedAt to be preserved")
		}
		if got.UpdatedAt.Before(created.UpdatedAt) {
			t.Error("expected UpdatedAt not to move backwards")
		}
		if !hasJob(t, s, created.ID) {
			t.Error("expected the job to still be registered")
		}
		if len(jobKeys(t, s)) != 1 {
			t.Errorf("expected exactly 1 job after an update, got %v", jobKeys(t, s))
		}
		stored, _ := st.Get(ctx, created.ID)
		if string(stored.Params) != `{"full":true}` {
			t.Errorf("expected the new params to be persisted, got %s", stored.Params)
		}
	})

	t.Run("disabling deregisters the job", func(t *testing.T) {
		s, _, _ := newTestScheduler(t)
		created, _ := s.Create(ctx, Schedule{TaskName: "scan", Cron: "0 2 * * *", Enabled: true})
		updated := created.Schedule
		updated.Enabled = false
		if _, err := s.Update(ctx, updated); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if hasJob(t, s, created.ID) {
			t.Error("expected the job to be deregistered")
		}
	})

	t.Run("reports a missing schedule", func(t *testing.T) {
		s, _, _ := newTestScheduler(t)
		_, err := s.Update(ctx, Schedule{ID: uuid.New(), TaskName: "scan", Cron: "0 2 * * *", Enabled: true})
		if !errors.Is(err, ErrScheduleNotFound) {
			t.Errorf("expected ErrScheduleNotFound, got %v", err)
		}
		_, err = s.Update(ctx, Schedule{TaskName: "scan", Cron: "0 2 * * *", Enabled: true})
		if !errors.Is(err, ErrScheduleNotFound) {
			t.Errorf("expected ErrScheduleNotFound for a nil id, got %v", err)
		}
	})

	t.Run("rejects an invalid cron and leaves the stored schedule alone", func(t *testing.T) {
		s, st, _ := newTestScheduler(t)
		created, _ := s.Create(ctx, Schedule{TaskName: "scan", Cron: "0 2 * * *", Enabled: true})
		updated := created.Schedule
		updated.Cron = "not a cron"
		if _, err := s.Update(ctx, updated); err == nil {
			t.Fatal("expected an error for an invalid cron")
		}
		stored, _ := st.Get(ctx, created.ID)
		if stored.Cron != "0 0 2 * * *" {
			t.Errorf("expected the stored cron to be untouched, got %q", stored.Cron)
		}
	})

}

func TestUpdateRestoresThePreviousRecordWhenRegistrationFails(t *testing.T) {
	ctx := context.Background()
	st := NewMemStore()
	s, err := New(Cfg{Store: st, Enqueuer: &fakeEnqueuer{}, Logger: quietLogger()})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := s.Start(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Cleanup(func() { _ = s.ShutDown(ctx) })
	s.newTrigger = func(_ string) (quartz.Trigger, error) { return quartz.NewSimpleTrigger(time.Hour), nil }

	created, err := s.Create(ctx, Schedule{TaskName: "scan", Cron: "0 2 * * *", Enabled: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	s.newTrigger = func(expr string) (quartz.Trigger, error) {
		if expr == "0 0 5 * * *" {
			return nil, errors.New("trigger boom")
		}
		return quartz.NewSimpleTrigger(time.Hour), nil
	}
	updated := created.Schedule
	updated.Cron = "0 5 * * *"
	if _, err := s.Update(ctx, updated); err == nil {
		t.Fatal("expected the update to fail")
	}
	stored, _ := st.Get(ctx, created.ID)
	if stored.Cron != "0 0 2 * * *" {
		t.Errorf("expected the previous cron to be restored, got %q", stored.Cron)
	}
	if !hasJob(t, s, created.ID) {
		t.Error("expected the previous job to be restored")
	}
}

func TestSetEnabled(t *testing.T) {
	ctx := context.Background()

	t.Run("toggles registration both ways", func(t *testing.T) {
		s, st, _ := newTestScheduler(t)
		created, _ := s.Create(ctx, Schedule{TaskName: "scan", Cron: "0 2 * * *", Enabled: true})

		off, err := s.SetEnabled(ctx, created.ID, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if off.Enabled {
			t.Error("expected Enabled=false")
		}
		if hasJob(t, s, created.ID) {
			t.Error("expected the job to be deregistered")
		}
		stored, _ := st.Get(ctx, created.ID)
		if stored.Enabled {
			t.Error("expected Enabled=false to be persisted")
		}

		on, err := s.SetEnabled(ctx, created.ID, true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !on.Enabled {
			t.Error("expected Enabled=true")
		}
		if !hasJob(t, s, created.ID) {
			t.Error("expected the job to be registered again")
		}
	})

	t.Run("setting the current value is a no-op", func(t *testing.T) {
		s, _, _ := newTestScheduler(t)
		created, _ := s.Create(ctx, Schedule{TaskName: "scan", Cron: "0 2 * * *", Enabled: true})
		got, err := s.SetEnabled(ctx, created.ID, true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !got.UpdatedAt.Equal(created.UpdatedAt) {
			t.Error("expected UpdatedAt not to change on a no-op")
		}
		if len(jobKeys(t, s)) != 1 {
			t.Errorf("expected exactly 1 job, got %v", jobKeys(t, s))
		}
	})

	t.Run("reports a missing schedule", func(t *testing.T) {
		s, _, _ := newTestScheduler(t)
		if _, err := s.SetEnabled(ctx, uuid.New(), true); !errors.Is(err, ErrScheduleNotFound) {
			t.Errorf("expected ErrScheduleNotFound, got %v", err)
		}
	})
}

func TestDelete(t *testing.T) {
	ctx := context.Background()

	t.Run("removes the schedule and its job", func(t *testing.T) {
		s, st, _ := newTestScheduler(t)
		created, _ := s.Create(ctx, Schedule{TaskName: "scan", Cron: "0 2 * * *", Enabled: true})
		if err := s.Delete(ctx, created.ID); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if hasJob(t, s, created.ID) {
			t.Error("expected the job to be deregistered")
		}
		if _, err := st.Get(ctx, created.ID); !errors.Is(err, ErrScheduleNotFound) {
			t.Errorf("expected the schedule to be gone, got %v", err)
		}
	})

	t.Run("deleting a disabled schedule works", func(t *testing.T) {
		s, _, _ := newTestScheduler(t)
		created, _ := s.Create(ctx, Schedule{TaskName: "scan", Cron: "0 2 * * *", Enabled: false})
		if err := s.Delete(ctx, created.ID); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("reports a missing schedule", func(t *testing.T) {
		s, _, _ := newTestScheduler(t)
		if err := s.Delete(ctx, uuid.New()); !errors.Is(err, ErrScheduleNotFound) {
			t.Errorf("expected ErrScheduleNotFound, got %v", err)
		}
	})

	t.Run("restores the job when the store delete fails", func(t *testing.T) {
		wantErr := errors.New("db is readonly")
		st := &storeWithFailingDelete{MemStore: NewMemStore(), deleteErr: wantErr}
		s, err := New(Cfg{Store: st, Enqueuer: &fakeEnqueuer{}, Logger: quietLogger()})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		s.newTrigger = func(_ string) (quartz.Trigger, error) {
			return quartz.NewSimpleTrigger(time.Hour), nil
		}
		if err := s.Start(ctx); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		t.Cleanup(func() { _ = s.ShutDown(ctx) })

		created, err := s.Create(ctx, Schedule{TaskName: "scan", Cron: "0 2 * * *", Enabled: true})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if err := s.Delete(ctx, created.ID); !errors.Is(err, wantErr) {
			t.Errorf("expected the store error to be wrapped, got %v", err)
		}
		if !hasJob(t, s, created.ID) {
			t.Error("expected the job to be restored after the failed delete")
		}
	})
}

func TestTrigger(t *testing.T) {
	ctx := context.Background()

	t.Run("enqueues the task with the stored params", func(t *testing.T) {
		s, _, enq := newTestScheduler(t)
		created, _ := s.Create(ctx, Schedule{
			TaskName: "scan",
			Cron:     "0 2 * * *",
			Params:   json.RawMessage(`{"full":true}`),
			Enabled:  true,
		})
		taskID, err := s.Trigger(ctx, created.ID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if taskID == uuid.Nil {
			t.Error("expected a task id")
		}
		calls := enq.snapshot()
		if len(calls) != 1 {
			t.Fatalf("expected 1 enqueue, got %d", len(calls))
		}
		if calls[0].name != "scan" || string(calls[0].params) != `{"full":true}` {
			t.Errorf("unexpected enqueue: %+v", calls[0])
		}
	})

	t.Run("works for a disabled schedule", func(t *testing.T) {
		s, _, enq := newTestScheduler(t)
		created, _ := s.Create(ctx, Schedule{TaskName: "scan", Cron: "0 2 * * *", Enabled: false})
		if _, err := s.Trigger(ctx, created.ID); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(enq.snapshot()) != 1 {
			t.Error("expected the task to be enqueued even though the schedule is disabled")
		}
	})

	t.Run("reports a missing schedule", func(t *testing.T) {
		s, _, _ := newTestScheduler(t)
		if _, err := s.Trigger(ctx, uuid.New()); !errors.Is(err, ErrScheduleNotFound) {
			t.Errorf("expected ErrScheduleNotFound, got %v", err)
		}
	})
}
