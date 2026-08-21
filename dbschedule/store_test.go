package dbschedule

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/go-bumbu/tempo/schedule"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{Logger: logger.Discard})
	if err != nil {
		t.Fatalf("failed to open test db: %v", err)
	}
	st, err := New(db)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return st
}

func TestNew(t *testing.T) {
	t.Run("requires a db", func(t *testing.T) {
		if _, err := New(nil); err == nil {
			t.Error("expected an error for a nil db")
		}
	})
	t.Run("migrates and satisfies schedule.Store", func(t *testing.T) {
		var _ schedule.Store = newTestStore(t)
	})
	t.Run("is idempotent", func(t *testing.T) {
		db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{Logger: logger.Discard})
		if err != nil {
			t.Fatalf("failed to open test db: %v", err)
		}
		if _, err := New(db); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, err := New(db); err != nil {
			t.Errorf("expected a second New to succeed, got %v", err)
		}
	})
}

func TestStoreRoundTrip(t *testing.T) {
	ctx := context.Background()

	t.Run("every field survives a round trip", func(t *testing.T) {
		st := newTestStore(t)
		want := schedule.Schedule{
			ID:        uuid.New(),
			TaskName:  "scan",
			Cron:      "0 0 2 * * *",
			Params:    json.RawMessage(`{"full":true}`),
			Enabled:   true,
			CreatedAt: time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC),
			UpdatedAt: time.Date(2026, 8, 21, 11, 0, 0, 0, time.UTC),
		}
		if err := st.Save(ctx, want); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got, err := st.Get(ctx, want.ID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// Compare timestamps in UTC; sqlite round-trips the instant, not the
		// monotonic clock or location.
		want.CreatedAt = want.CreatedAt.UTC()
		want.UpdatedAt = want.UpdatedAt.UTC()
		got.CreatedAt = got.CreatedAt.UTC()
		got.UpdatedAt = got.UpdatedAt.UTC()
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("schedule mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("nil params round-trip as nil", func(t *testing.T) {
		st := newTestStore(t)
		id := uuid.New()
		if err := st.Save(ctx, schedule.Schedule{ID: id, TaskName: "scan", Cron: "0 * * * * *"}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got, err := st.Get(ctx, id)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.Params != nil {
			t.Errorf("expected nil params, got %s", got.Params)
		}
	})

	t.Run("timestamps are not overwritten by gorm", func(t *testing.T) {
		st := newTestStore(t)
		created := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
		id := uuid.New()
		if err := st.Save(ctx, schedule.Schedule{
			ID: id, TaskName: "scan", Cron: "0 * * * * *",
			CreatedAt: created, UpdatedAt: created,
		}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got, err := st.Get(ctx, id)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !got.CreatedAt.UTC().Equal(created) {
			t.Errorf("expected CreatedAt %v, got %v", created, got.CreatedAt.UTC())
		}
		if !got.UpdatedAt.UTC().Equal(created) {
			t.Errorf("expected UpdatedAt %v, got %v", created, got.UpdatedAt.UTC())
		}
	})
}

func TestStoreSaveIsAnUpsert(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	id := uuid.New()

	if err := st.Save(ctx, schedule.Schedule{ID: id, TaskName: "scan", Cron: "0 * * * * *", Enabled: true}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := st.Save(ctx, schedule.Schedule{ID: id, TaskName: "scan", Cron: "0 0 * * * *", Enabled: false}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, err := st.Get(ctx, id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Cron != "0 0 * * * *" {
		t.Errorf("expected the cron to be replaced, got %q", got.Cron)
	}
	// The apps' old store had a `default:true` column tag, which made gorm
	// substitute the default for a zero-value false on insert. Guard that.
	if got.Enabled {
		t.Error("expected Enabled=false to persist")
	}
	list, _ := st.List(ctx)
	if len(list) != 1 {
		t.Errorf("expected 1 schedule after an upsert, got %d", len(list))
	}
}

func TestStoreSeveralSchedulesShareATaskName(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)

	daily := schedule.Schedule{ID: uuid.New(), TaskName: "scan", Cron: "0 0 2 * * *", Enabled: true, CreatedAt: time.Unix(1, 0)}
	weekly := schedule.Schedule{ID: uuid.New(), TaskName: "scan", Cron: "0 0 3 * * 0", Enabled: true, CreatedAt: time.Unix(2, 0)}
	if err := st.Save(ctx, daily); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := st.Save(ctx, weekly); err != nil {
		t.Fatalf("expected two schedules for one task name to be allowed: %v", err)
	}

	list, err := st.List(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 schedules, got %d", len(list))
	}
	if list[0].ID != daily.ID || list[1].ID != weekly.ID {
		t.Errorf("expected CreatedAt order, got %v then %v", list[0].ID, list[1].ID)
	}
}

func TestStoreMissingSchedules(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)

	if _, err := st.Get(ctx, uuid.New()); !errors.Is(err, schedule.ErrScheduleNotFound) {
		t.Errorf("Get: expected ErrScheduleNotFound, got %v", err)
	}
	if err := st.Delete(ctx, uuid.New()); !errors.Is(err, schedule.ErrScheduleNotFound) {
		t.Errorf("Delete: expected ErrScheduleNotFound, got %v", err)
	}
}

func TestStoreDeleteIsHard(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	id := uuid.New()

	if err := st.Save(ctx, schedule.Schedule{ID: id, TaskName: "scan", Cron: "0 * * * * *"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := st.Delete(ctx, id); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// A hard delete means the same id can be created again with no unique or
	// soft-delete collision — the trap the apps worked around with Unscoped().
	if err := st.Save(ctx, schedule.Schedule{ID: id, TaskName: "scan", Cron: "0 0 * * * *"}); err != nil {
		t.Errorf("expected the id to be reusable after a delete, got %v", err)
	}
	list, _ := st.List(ctx)
	if len(list) != 1 {
		t.Errorf("expected 1 schedule, got %d", len(list))
	}
}

func TestStoreListIsEmptyByDefault(t *testing.T) {
	st := newTestStore(t)
	list, err := st.List(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(list) != 0 {
		t.Errorf("expected no schedules, got %d", len(list))
	}
}

func TestStoreTimestampsAreStoredAsInstants(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)

	// Two rows created at the same instant but handed over in different zones.
	// Stored in the caller's zone, sqlite's text timestamps would sort these by
	// wall clock — "2026-08-21 20:00:00+10:00" after "2026-08-21 11:00:00Z" —
	// putting them in the wrong order. Stored in UTC they sort by instant.
	instant := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	first := schedule.Schedule{
		ID: uuid.New(), TaskName: "first", Cron: "0 * * * * *",
		CreatedAt: instant.In(time.FixedZone("east", 10*60*60)),
	}
	second := schedule.Schedule{
		ID: uuid.New(), TaskName: "second", Cron: "0 * * * * *",
		CreatedAt: instant.Add(time.Hour),
	}
	if err := st.Save(ctx, first); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := st.Save(ctx, second); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	list, err := st.List(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 schedules, got %d", len(list))
	}
	if list[0].TaskName != "first" || list[1].TaskName != "second" {
		t.Errorf("expected instant order [first second], got [%s %s]", list[0].TaskName, list[1].TaskName)
	}
	if !list[0].CreatedAt.Equal(instant) {
		t.Errorf("expected the instant to survive the round trip, got %v", list[0].CreatedAt)
	}
}

// recordingEnqueuer is a schedule.Enqueuer that records what it was handed. The
// schedule package's own fixtures live in its test files, which are not
// importable from here, so the cross-package tests below build their own.
type recordingEnqueuer struct {
	mu    sync.Mutex
	calls []string
}

func (r *recordingEnqueuer) AddRaw(name string, params []byte) (uuid.UUID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, name+" "+string(params))
	return uuid.New(), nil
}

func (r *recordingEnqueuer) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

// newTestScheduler wires a real schedule.Scheduler over a gorm-backed Store on a
// fresh in-memory sqlite database. This is the seam neither package can cover on
// its own: schedule's tests all run against MemStore, and dbschedule's exercise
// the Store interface without a Scheduler above it.
//
// The triggers are the real cron ones — nothing here waits for a fire, so cron's
// one-second granularity does not matter, and Trigger enqueues without one.
func newTestScheduler(t *testing.T) (*schedule.Scheduler, *Store, *recordingEnqueuer) {
	t.Helper()
	st := newTestStore(t)
	enq := &recordingEnqueuer{}
	sched, err := schedule.New(schedule.Cfg{
		Store:    st,
		Enqueuer: enq,
		Logger:   slog.New(slog.DiscardHandler),
		Location: time.UTC,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := sched.Start(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = sched.ShutDown(ctx)
	})
	return sched, st, enq
}

// mustCreate creates one enabled schedule and fails the test if it cannot.
func mustCreate(t *testing.T, sched *schedule.Scheduler, params string) schedule.ScheduleInfo {
	t.Helper()
	created, err := sched.Create(context.Background(), schedule.Schedule{
		TaskName: "scan",
		Cron:     "0 2 * * *",
		Params:   json.RawMessage(params),
		Enabled:  true,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	return created
}

func TestSchedulerCreateOverAGormStore(t *testing.T) {
	ctx := context.Background()
	sched, st, _ := newTestScheduler(t)

	created := mustCreate(t, sched, `{"full":false}`)
	if created.Cron != "0 0 2 * * *" {
		t.Errorf("expected the normalized cron, got %q", created.Cron)
	}
	if created.NextFireAt == nil {
		t.Error("expected a next fire time for an enabled schedule")
	}

	row, err := st.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("expected the schedule to reach the database: %v", err)
	}
	if row.Cron != "0 0 2 * * *" {
		t.Errorf("expected the normalized cron in the database, got %q", row.Cron)
	}
	if string(row.Params) != `{"full":false}` {
		t.Errorf("expected the params in the database, got %s", row.Params)
	}
	if !row.Enabled {
		t.Error("expected Enabled=true in the database")
	}
}

func TestSchedulerCreateRejectsADuplicateIdOverAGormStore(t *testing.T) {
	ctx := context.Background()
	sched, _, _ := newTestScheduler(t)

	created := mustCreate(t, sched, `{"full":false}`)
	_, err := sched.Create(ctx, schedule.Schedule{
		ID: created.ID, TaskName: "other", Cron: "0 4 * * *", Enabled: true,
	})
	if !errors.Is(err, schedule.ErrScheduleExists) {
		t.Errorf("expected ErrScheduleExists, got %v", err)
	}
}

func TestSchedulerUpdateOverAGormStore(t *testing.T) {
	ctx := context.Background()
	sched, st, _ := newTestScheduler(t)

	created := mustCreate(t, sched, `{"full":false}`)
	before, err := st.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	updated := created.Schedule
	updated.Cron = "0 5 * * *"
	updated.Params = json.RawMessage(`{"full":true}`)
	if _, err := sched.Update(ctx, updated); err != nil {
		t.Fatalf("Update: %v", err)
	}

	row, err := st.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if row.Cron != "0 0 5 * * *" {
		t.Errorf("expected the new normalized cron in the database, got %q", row.Cron)
	}
	if string(row.Params) != `{"full":true}` {
		t.Errorf("expected the new params in the database, got %s", row.Params)
	}
	// Compare the two values that made the same round trip, so sqlite's timestamp
	// precision cannot make this flaky.
	if !row.CreatedAt.Equal(before.CreatedAt) {
		t.Errorf("expected CreatedAt to survive an Update: %v became %v", before.CreatedAt, row.CreatedAt)
	}
}

func TestSchedulerSetEnabledOverAGormStore(t *testing.T) {
	ctx := context.Background()
	sched, st, _ := newTestScheduler(t)

	created := mustCreate(t, sched, `{"full":false}`)
	off, err := sched.SetEnabled(ctx, created.ID, false)
	if err != nil {
		t.Fatalf("SetEnabled: %v", err)
	}
	if off.Enabled {
		t.Error("expected Enabled=false")
	}
	if off.NextFireAt != nil {
		t.Error("expected no next fire time for a disabled schedule")
	}
	row, err := st.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if row.Enabled {
		t.Error("expected Enabled=false in the database")
	}

	on, err := sched.SetEnabled(ctx, created.ID, true)
	if err != nil {
		t.Fatalf("SetEnabled: %v", err)
	}
	if on.NextFireAt == nil {
		t.Error("expected a next fire time once re-enabled")
	}
}

func TestSchedulerTriggerOverAGormStore(t *testing.T) {
	ctx := context.Background()
	sched, _, enq := newTestScheduler(t)

	created := mustCreate(t, sched, `{"full":true}`)
	// Disabled on purpose: Trigger reads the store and enqueues regardless.
	if _, err := sched.SetEnabled(ctx, created.ID, false); err != nil {
		t.Fatalf("SetEnabled: %v", err)
	}
	taskID, err := sched.Trigger(ctx, created.ID)
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if taskID == uuid.Nil {
		t.Error("expected a task id")
	}
	calls := enq.snapshot()
	if len(calls) != 1 || calls[0] != `scan {"full":true}` {
		t.Errorf("expected one enqueue with the params from the database, got %v", calls)
	}
}

func TestSchedulerReloadOverAGormStore(t *testing.T) {
	ctx := context.Background()
	sched, st, _ := newTestScheduler(t)

	// A row written straight to the database, as a restore or manual SQL would.
	outOfBand := schedule.Schedule{
		ID: uuid.New(), TaskName: "scan", Cron: "0 0 6 * * *", Enabled: true,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
	if err := st.Save(ctx, outOfBand); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got, err := sched.Get(ctx, outOfBand.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.NextFireAt != nil {
		t.Error("a direct database write should not register a job on its own")
	}

	if err := sched.Reload(ctx); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	got, err = sched.Get(ctx, outOfBand.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.NextFireAt == nil {
		t.Error("expected Reload to register the row written out of band")
	}
}

func TestSchedulerDeleteOverAGormStore(t *testing.T) {
	ctx := context.Background()
	sched, st, _ := newTestScheduler(t)

	created := mustCreate(t, sched, `{"full":false}`)
	if err := sched.Delete(ctx, created.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := st.Get(ctx, created.ID); !errors.Is(err, schedule.ErrScheduleNotFound) {
		t.Errorf("expected the row to be gone from the database, got %v", err)
	}
	if _, err := sched.Get(ctx, created.ID); !errors.Is(err, schedule.ErrScheduleNotFound) {
		t.Errorf("expected the Scheduler to report the schedule gone, got %v", err)
	}
	if err := sched.Delete(ctx, created.ID); !errors.Is(err, schedule.ErrScheduleNotFound) {
		t.Errorf("expected a second Delete to report ErrScheduleNotFound, got %v", err)
	}
}
