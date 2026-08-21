package dbqueue

import (
	"context"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/go-bumbu/tempo"
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

// find returns the persisted task with id, reading it back through List since
// the store has no Get.
func find(t *testing.T, st *Store, id uuid.UUID) (tempo.TaskInfo, bool) {
	t.Helper()
	list, err := st.List(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, ti := range list {
		if ti.ID == id {
			return ti, true
		}
	}
	return tempo.TaskInfo{}, false
}

func TestNew(t *testing.T) {
	t.Run("requires a db", func(t *testing.T) {
		if _, err := New(nil); err == nil {
			t.Error("expected an error for a nil db")
		}
	})
	t.Run("migrates and satisfies RecoverablePersistence", func(t *testing.T) {
		var _ tempo.RecoverablePersistence = newTestStore(t)
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
		want := tempo.TaskInfo{
			ID:        uuid.New(),
			Name:      "scan",
			Status:    tempo.TaskStatusComplete,
			QueuedAt:  time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC),
			StartedAt: time.Date(2026, 8, 21, 10, 0, 1, 0, time.UTC),
			EndedAt:   time.Date(2026, 8, 21, 10, 0, 2, 0, time.UTC),
			Params:    []byte(`{"full":true}`),
		}
		if err := st.SaveTask(ctx, want); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got, ok := find(t, st, want.ID)
		if !ok {
			t.Fatalf("saved task not returned by List")
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("task mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("zero times survive for a waiting task", func(t *testing.T) {
		st := newTestStore(t)
		id := uuid.New()
		// A freshly enqueued task is Waiting with no start/end time. The core
		// relies on IsZero() to tell an unstarted task from a finished one and to
		// backfill EndedAt on crash recovery, so the zero must round-trip as zero.
		if err := st.SaveTask(ctx, tempo.TaskInfo{
			ID: id, Name: "scan", Status: tempo.TaskStatusWaiting,
			QueuedAt: time.Now().UTC(),
		}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got, ok := find(t, st, id)
		if !ok {
			t.Fatalf("saved task not returned by List")
		}
		if !got.StartedAt.IsZero() {
			t.Errorf("expected zero StartedAt, got %v", got.StartedAt)
		}
		if !got.EndedAt.IsZero() {
			t.Errorf("expected zero EndedAt, got %v", got.EndedAt)
		}
	})

	t.Run("nil params round-trip as nil", func(t *testing.T) {
		st := newTestStore(t)
		id := uuid.New()
		if err := st.SaveTask(ctx, tempo.TaskInfo{ID: id, Name: "scan", Status: tempo.TaskStatusWaiting, QueuedAt: time.Now().UTC()}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got, ok := find(t, st, id)
		if !ok {
			t.Fatalf("saved task not returned by List")
		}
		if got.Params != nil {
			t.Errorf("expected nil params, got %s", got.Params)
		}
	})
}

func TestStoreSaveTaskIsAnUpsert(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	id := uuid.New()

	if err := st.SaveTask(ctx, tempo.TaskInfo{ID: id, Name: "scan", Status: tempo.TaskStatusWaiting, QueuedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ended := time.Now().UTC()
	if err := st.SaveTask(ctx, tempo.TaskInfo{ID: id, Name: "scan", Status: tempo.TaskStatusComplete, QueuedAt: time.Now().UTC(), EndedAt: ended}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, ok := find(t, st, id)
	if !ok {
		t.Fatalf("saved task not returned by List")
	}
	if got.Status != tempo.TaskStatusComplete {
		t.Errorf("expected the status to be replaced, got %s", got.Status.Str())
	}
	list, _ := st.List(ctx)
	if len(list) != 1 {
		t.Errorf("expected 1 task after an upsert, got %d", len(list))
	}
}

func TestStoreRemoveTasks(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)

	keep := uuid.New()
	drop1 := uuid.New()
	drop2 := uuid.New()
	for _, id := range []uuid.UUID{keep, drop1, drop2} {
		if err := st.SaveTask(ctx, tempo.TaskInfo{ID: id, Name: "scan", Status: tempo.TaskStatusComplete, QueuedAt: time.Now().UTC()}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if err := st.RemoveTasks(ctx, []uuid.UUID{drop1, drop2}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	list, err := st.List(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(list) != 1 || list[0].ID != keep {
		t.Errorf("expected only the kept task to remain, got %v", list)
	}
}

func TestStoreRemoveTasksEmptyIsANoOp(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	if err := st.SaveTask(ctx, tempo.TaskInfo{ID: uuid.New(), Name: "scan", Status: tempo.TaskStatusComplete, QueuedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := st.RemoveTasks(ctx, nil); err != nil {
		t.Errorf("expected removing nothing to be a no-op, got %v", err)
	}
	list, _ := st.List(ctx)
	if len(list) != 1 {
		t.Errorf("expected the task to remain, got %d", len(list))
	}
}

func TestStoreListIsEmptyByDefault(t *testing.T) {
	list, err := newTestStore(t).List(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(list) != 0 {
		t.Errorf("expected no tasks, got %d", len(list))
	}
}

func TestStoreListIsOrderedByQueuedAtInstant(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)

	// recoverTasks appends tasks in List order and NextTask claims the first
	// Waiting one, so List must return oldest-queued first to keep FIFO claim
	// order across a restart. Stored in the caller's zone, sqlite's text
	// timestamps would sort "20:00:00+10:00" after "11:00:00Z"; stored in UTC
	// they sort by instant.
	instant := time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)
	first := tempo.TaskInfo{
		ID: uuid.New(), Name: "first", Status: tempo.TaskStatusWaiting,
		QueuedAt: instant.In(time.FixedZone("east", 10*60*60)),
	}
	second := tempo.TaskInfo{
		ID: uuid.New(), Name: "second", Status: tempo.TaskStatusWaiting,
		QueuedAt: instant.Add(time.Hour),
	}
	// Saved newest-first to prove List reorders rather than echoing insert order.
	if err := st.SaveTask(ctx, second); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := st.SaveTask(ctx, first); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	list, err := st.List(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 tasks, got %d", len(list))
	}
	if list[0].Name != "first" || list[1].Name != "second" {
		t.Errorf("expected instant order [first second], got [%s %s]", list[0].Name, list[1].Name)
	}
	if !list[0].QueuedAt.Equal(instant) {
		t.Errorf("expected the instant to survive the round trip, got %v", list[0].QueuedAt)
	}
}
