package dbschedule

import (
	"context"
	"encoding/json"
	"errors"
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
