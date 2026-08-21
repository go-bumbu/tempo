package dbqueue

import (
	"context"
	"testing"
	"time"

	"github.com/go-bumbu/tempo"
	"github.com/google/uuid"
)

// TestQueueRecoversPersistedTasks is the seam neither package can cover alone:
// dbqueue's store tests never build a queue, and the core's recovery tests run
// against in-memory persistence. It proves a fresh queue over a populated store
// reloads its tasks — the whole reason dbqueue exists.
func TestQueueRecoversPersistedTasks(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	// State persisted before a crash: one task still Waiting, one caught Running.
	waiting := tempo.TaskInfo{
		ID: uuid.New(), Name: "scan", Status: tempo.TaskStatusWaiting,
		QueuedAt: time.Now().UTC(), Params: []byte(`{"full":true}`),
	}
	running := tempo.TaskInfo{
		ID: uuid.New(), Name: "scan", Status: tempo.TaskStatusRunning,
		QueuedAt: time.Now().UTC(), StartedAt: time.Now().UTC(),
	}
	if err := store.SaveTask(ctx, waiting); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := store.SaveTask(ctx, running); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// "Restart": a fresh queue over the same store.
	q := tempo.NewTaskQueue(tempo.TaskQueueCfg{QueueSize: 10, Persistence: store})

	// The waiting task survives and is claimable, with its params intact.
	claimCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	id, name, params, err := q.NextTask(claimCtx, nil)
	if err != nil {
		t.Fatalf("expected a claimable recovered task, got %v", err)
	}
	if id != waiting.ID || name != "scan" || string(params) != `{"full":true}` {
		t.Errorf("recovered the wrong task: id=%s name=%s params=%s", id, name, params)
	}

	// The task caught Running is reconciled to Failed (terminal), not left to run,
	// and EndedAt is backfilled.
	got, err := q.Get(ctx, running.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Status != tempo.TaskStatusFailed {
		t.Errorf("expected the orphaned Running task to recover as Failed, got %s", got.Status.Str())
	}
	if got.EndedAt.IsZero() {
		t.Error("expected recovery to backfill EndedAt on the failed task")
	}

	// The reconciliation is written back, so a second restart stays consistent.
	persisted, ok := find(t, store, running.ID)
	if !ok || persisted.Status != tempo.TaskStatusFailed {
		t.Errorf("expected Failed to be persisted, got %+v (found=%v)", persisted, ok)
	}
}

// TestRunnerRunsRecoveredWaitingTask is the end-to-end proof: a task enqueued
// before a restart actually executes after one, through a real QueueRunner.
func TestRunnerRunsRecoveredWaitingTask(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	enqueued := tempo.TaskInfo{
		ID: uuid.New(), Name: "scan", Status: tempo.TaskStatusWaiting,
		QueuedAt: time.Now().UTC(),
	}
	if err := store.SaveTask(ctx, enqueued); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// A fresh runner over the same store, as a process would build on startup.
	runner, err := tempo.NewQueueRunner(tempo.RunnerCfg{
		Parallelism: 1, QueueSize: 10, Persistence: store,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ran := make(chan uuid.UUID, 1)
	runner.RegisterRaw("scan", func(ctx context.Context, _ []byte) error {
		ran <- enqueued.ID
		return nil
	})

	runner.StartBg()
	t.Cleanup(func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = runner.ShutDown(shutCtx)
	})

	select {
	case got := <-ran:
		if got != enqueued.ID {
			t.Errorf("ran the wrong task: got %s, want %s", got, enqueued.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the recovered waiting task never ran")
	}
}
