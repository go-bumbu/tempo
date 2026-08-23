package tempo

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

// fakeCleaner is a TaskLogSink that also records cleanup calls.
type fakeCleaner struct {
	appended int
	removed  []uuid.UUID
	retained [][]uuid.UUID
}

func (f *fakeCleaner) Append(context.Context, uuid.UUID, string, string) error {
	f.appended++
	return nil
}
func (f *fakeCleaner) RemoveTasks(_ context.Context, ids []uuid.UUID) error {
	f.removed = append(f.removed, ids...)
	return nil
}
func (f *fakeCleaner) RetainOnly(_ context.Context, keep []uuid.UUID) error {
	f.retained = append(f.retained, keep)
	return nil
}

// fakeRecoverable recovers a fixed task list, preserving the given order.
//
// queue_test.go already defines recoverableMemPersistence, reused below for
// TestRetainOnlyAtConstruction where order doesn't matter. It is not reused
// here: it stores tasks in a map and List() ranges over that map, and Go
// deliberately randomizes map-iteration order (verified empirically: ranging
// over the same 3-entry map twice in a row can yield different orders). That
// makes it unfit for TestCleanupOnceForwardsRemovedIDs, which pins the exact
// order CleanHistory trims ids in.
type fakeRecoverable struct{ list []TaskInfo }

func (f *fakeRecoverable) SaveTask(context.Context, TaskInfo) error       { return nil }
func (f *fakeRecoverable) RemoveTasks(context.Context, []uuid.UUID) error { return nil }
func (f *fakeRecoverable) List(context.Context) ([]TaskInfo, error)       { return f.list, nil }

func term(id uuid.UUID) TaskInfo {
	return TaskInfo{ID: id, Name: "t", Status: TaskStatusComplete, EndedAt: time.Now()}
}

func TestRetainOnlyAtConstruction(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	fc := &fakeCleaner{}
	persist := newRecoverableMemPersistence()
	ctx := context.Background()
	if err := persist.SaveTask(ctx, term(a)); err != nil {
		t.Fatal(err)
	}
	if err := persist.SaveTask(ctx, term(b)); err != nil {
		t.Fatal(err)
	}
	_, err := NewQueueRunner(RunnerCfg{
		QueueSize: 10, HistorySize: 10,
		Persistence: persist,
		LogSink:     fc,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(fc.retained) != 1 || len(fc.retained[0]) != 2 {
		t.Fatalf("RetainOnly calls = %v, want one call keeping 2 ids", fc.retained)
	}
}

func TestRetainOnlyEmptyUnderMemPersistence(t *testing.T) {
	fc := &fakeCleaner{}
	_, err := NewQueueRunner(RunnerCfg{
		QueueSize: 10, HistorySize: 10,
		Persistence: NewMemPersistence(),
		LogSink:     fc,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(fc.retained) != 1 || len(fc.retained[0]) != 0 {
		t.Fatalf("RetainOnly calls = %v, want one call keeping 0 ids", fc.retained)
	}
}

func TestCleanupOnceForwardsRemovedIDs(t *testing.T) {
	a, b, c := uuid.New(), uuid.New(), uuid.New()
	fc := &fakeCleaner{}
	r, err := NewQueueRunner(RunnerCfg{
		QueueSize: 10, HistorySize: 1, // keep 1 of 3 terminal -> remove 2
		Persistence: &fakeRecoverable{list: []TaskInfo{term(a), term(b), term(c)}},
		LogSink:     fc,
	})
	if err != nil {
		t.Fatal(err)
	}
	r.cleanupOnce(context.Background())
	if len(fc.removed) != 2 || fc.removed[0] != a || fc.removed[1] != b {
		t.Fatalf("removed = %v, want [%v %v]", fc.removed, a, b)
	}
}

// fakeProgressCleaner is a TaskProgressSink that also records cleanup calls.
type fakeProgressCleaner struct {
	set      int
	removed  []uuid.UUID
	retained [][]uuid.UUID
}

func (f *fakeProgressCleaner) Set(context.Context, uuid.UUID, ProgressState) error {
	f.set++
	return nil
}
func (f *fakeProgressCleaner) RemoveTasks(_ context.Context, ids []uuid.UUID) error {
	f.removed = append(f.removed, ids...)
	return nil
}
func (f *fakeProgressCleaner) RetainOnly(_ context.Context, keep []uuid.UUID) error {
	f.retained = append(f.retained, keep)
	return nil
}

func TestProgressRetainOnlyAtConstruction(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	fpc := &fakeProgressCleaner{}
	_, err := NewQueueRunner(RunnerCfg{
		QueueSize: 10, HistorySize: 10,
		Persistence:  &fakeRecoverable{list: []TaskInfo{term(a), term(b)}},
		ProgressSink: fpc,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(fpc.retained) != 1 || len(fpc.retained[0]) != 2 {
		t.Fatalf("RetainOnly calls = %v, want one call keeping 2 ids", fpc.retained)
	}
}

func TestProgressRetainOnlyEmptyUnderMemPersistence(t *testing.T) {
	fpc := &fakeProgressCleaner{}
	_, err := NewQueueRunner(RunnerCfg{
		QueueSize: 10, HistorySize: 10,
		Persistence:  NewMemPersistence(),
		ProgressSink: fpc,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(fpc.retained) != 1 || len(fpc.retained[0]) != 0 {
		t.Fatalf("RetainOnly calls = %v, want one call keeping 0 ids", fpc.retained)
	}
}

func TestProgressCleanupOnceForwardsRemovedIDs(t *testing.T) {
	a, b, c := uuid.New(), uuid.New(), uuid.New()
	fpc := &fakeProgressCleaner{}
	r, err := NewQueueRunner(RunnerCfg{
		QueueSize: 10, HistorySize: 1, // keep 1 of 3 terminal -> remove 2
		Persistence:  &fakeRecoverable{list: []TaskInfo{term(a), term(b), term(c)}},
		ProgressSink: fpc,
	})
	if err != nil {
		t.Fatal(err)
	}
	r.cleanupOnce(context.Background())
	if len(fpc.removed) != 2 || fpc.removed[0] != a || fpc.removed[1] != b {
		t.Fatalf("removed = %v, want [%v %v]", fpc.removed, a, b)
	}
}
