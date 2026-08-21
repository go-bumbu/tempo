package schedule

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
)

func TestMemStore(t *testing.T) {
	ctx := context.Background()

	t.Run("save then get round-trips every field", func(t *testing.T) {
		st := NewMemStore()
		want := Schedule{
			ID:        uuid.New(),
			TaskName:  "scan",
			Cron:      "0 0 2 * * *",
			Params:    json.RawMessage(`{"full":true}`),
			Enabled:   true,
			CreatedAt: time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC),
			UpdatedAt: time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC),
		}
		if err := st.Save(ctx, want); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got, err := st.Get(ctx, want.ID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("schedule mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("save is an upsert by id", func(t *testing.T) {
		st := NewMemStore()
		id := uuid.New()
		if err := st.Save(ctx, Schedule{ID: id, TaskName: "scan", Cron: "0 * * * * *", Enabled: true}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := st.Save(ctx, Schedule{ID: id, TaskName: "scan", Cron: "0 0 * * * *", Enabled: false}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got, err := st.Get(ctx, id)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.Cron != "0 0 * * * *" {
			t.Errorf("expected cron to be replaced, got %q", got.Cron)
		}
		if got.Enabled {
			t.Error("expected Enabled=false to persist")
		}
		list, _ := st.List(ctx)
		if len(list) != 1 {
			t.Errorf("expected 1 schedule after upsert, got %d", len(list))
		}
	})

	t.Run("get and delete report missing schedules", func(t *testing.T) {
		st := NewMemStore()
		if _, err := st.Get(ctx, uuid.New()); !errors.Is(err, ErrScheduleNotFound) {
			t.Errorf("expected ErrScheduleNotFound, got %v", err)
		}
		if err := st.Delete(ctx, uuid.New()); !errors.Is(err, ErrScheduleNotFound) {
			t.Errorf("expected ErrScheduleNotFound, got %v", err)
		}
	})

	t.Run("delete removes only the requested schedule", func(t *testing.T) {
		st := NewMemStore()
		keep := Schedule{ID: uuid.New(), TaskName: "keep", CreatedAt: time.Unix(1, 0)}
		drop := Schedule{ID: uuid.New(), TaskName: "drop", CreatedAt: time.Unix(2, 0)}
		_ = st.Save(ctx, keep)
		_ = st.Save(ctx, drop)
		if err := st.Delete(ctx, drop.ID); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		list, _ := st.List(ctx)
		if len(list) != 1 || list[0].TaskName != "keep" {
			t.Errorf("expected only \"keep\" to remain, got %+v", list)
		}
	})

	t.Run("list is ordered by CreatedAt then id", func(t *testing.T) {
		st := NewMemStore()
		older := Schedule{ID: uuid.New(), TaskName: "older", CreatedAt: time.Unix(100, 0)}
		newer := Schedule{ID: uuid.New(), TaskName: "newer", CreatedAt: time.Unix(200, 0)}
		_ = st.Save(ctx, newer)
		_ = st.Save(ctx, older)
		list, err := st.List(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(list) != 2 || list[0].TaskName != "older" || list[1].TaskName != "newer" {
			t.Errorf("expected [older newer], got %+v", list)
		}
	})

}

// TestMemStoreDoesNotAliasParams guards every door of the store. Params is the
// only field of a Schedule that assignment does not copy, so it is the only one
// that can alias — dbschedule is immune because serialization breaks the alias,
// and MemStore has to clone by hand to stay interchangeable with it.
func TestMemStoreDoesNotAliasParams(t *testing.T) {
	ctx := context.Background()

	t.Run("list returns copies, not aliases", func(t *testing.T) {
		st := NewMemStore()
		id := uuid.New()
		_ = st.Save(ctx, Schedule{ID: id, TaskName: "scan", Cron: "0 * * * * *", Params: json.RawMessage(`{"full":true}`)})
		list, _ := st.List(ctx)
		list[0].Cron = "mutated"
		list[0].Params[2] = 'X'
		got, _ := st.Get(ctx, id)
		if got.Cron != "0 * * * * *" {
			t.Errorf("mutating a listed schedule changed the store: %q", got.Cron)
		}
		if string(got.Params) != `{"full":true}` {
			t.Errorf("mutating a listed schedule's params changed the store: %s", got.Params)
		}
	})

	t.Run("get returns copies, not aliases", func(t *testing.T) {
		st := NewMemStore()
		id := uuid.New()
		_ = st.Save(ctx, Schedule{ID: id, TaskName: "scan", Cron: "0 * * * * *", Params: json.RawMessage(`{"full":true}`)})
		got, _ := st.Get(ctx, id)
		got.Params[2] = 'X'
		again, _ := st.Get(ctx, id)
		if string(again.Params) != `{"full":true}` {
			t.Errorf("mutating a fetched schedule's params changed the store: %s", again.Params)
		}
	})

	t.Run("save does not alias the caller's params", func(t *testing.T) {
		st := NewMemStore()
		id := uuid.New()
		params := json.RawMessage(`{"full":true}`)
		_ = st.Save(ctx, Schedule{ID: id, TaskName: "scan", Cron: "0 * * * * *", Params: params})
		params[2] = 'X'
		got, _ := st.Get(ctx, id)
		if string(got.Params) != `{"full":true}` {
			t.Errorf("mutating the saved slice changed the store: %s", got.Params)
		}
	})

	t.Run("nil params stay nil", func(t *testing.T) {
		st := NewMemStore()
		id := uuid.New()
		_ = st.Save(ctx, Schedule{ID: id, TaskName: "scan", Cron: "0 * * * * *"})
		got, _ := st.Get(ctx, id)
		if got.Params != nil {
			t.Errorf("expected nil params, got %s", got.Params)
		}
		list, _ := st.List(ctx)
		if list[0].Params != nil {
			t.Errorf("expected nil params from List, got %s", list[0].Params)
		}
	})
}

func TestMemStoreSatisfiesStore(t *testing.T) {
	var _ Store = NewMemStore()
}
