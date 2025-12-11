package tempo_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-bumbu/tempo"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"sync"
	"testing"
	"time"
)

// Helper to create a test queue
func newTestQueue(size int) *tempo.Queue {
	return tempo.NewQueue(tempo.QueueCfg{QueueSize: size})
}

func TestQueueAdd(t *testing.T) {
	t.Run("basic add operations", func(t *testing.T) {
		tq := newTestQueue(10)

		// Add single task
		id, err := tq.Add(func(ctx context.Context) {})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if id == uuid.Nil {
			t.Error("expected non-nil UUID")
		}

		// Add multiple tasks with unique IDs
		ids := make(map[uuid.UUID]bool)
		for i := 0; i < 5; i++ {
			id, err := tq.Add(func(ctx context.Context) {})
			if err != nil {
				t.Fatalf("error adding task %d: %v", i, err)
			}
			if ids[id] {
				t.Errorf("duplicate UUID: %v", id)
			}
			ids[id] = true
		}

		// Verify total count
		if len(tq.List()) != 6 {
			t.Errorf("expected 6 tasks, got %d", len(tq.List()))
		}
	})

	t.Run("queue full", func(t *testing.T) {
		tq := newTestQueue(3)

		for i := 0; i < 3; i++ {
			if _, err := tq.Add(func(ctx context.Context) {}); err != nil {
				t.Fatalf("error at task %d: %v", i, err)
			}
		}

		_, err := tq.Add(func(ctx context.Context) {})
		if !errors.Is(err, tempo.ErrQueueFull) {
			t.Errorf("expected ErrQueueFull, got: %v", err)
		}
	})

	t.Run("concurrent add", func(t *testing.T) {
		tq := newTestQueue(100)
		numTasks := 50

		var wg sync.WaitGroup
		for i := 0; i < numTasks; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = tq.Add(func(ctx context.Context) {})
			}()
		}
		wg.Wait()

		if len(tq.List()) != numTasks {
			t.Errorf("expected %d tasks, got %d", numTasks, len(tq.List()))
		}
	})
}

func TestQueueList(t *testing.T) {
	tq := newTestQueue(10)

	// Empty queue
	if len(tq.List()) != 0 {
		t.Error("expected empty list for new queue")
	}

	// Add tasks and verify
	for i := 0; i < 3; i++ {
		tq.Add(func(ctx context.Context) {})
	}

	tasks := tq.List()
	if len(tasks) != 3 {
		t.Fatalf("expected 3 tasks, got %d", len(tasks))
	}

	// Verify task info
	for i, task := range tasks {
		if task.Status != tempo.TaskStatusWaiting {
			t.Errorf("task %d: expected waiting status, got %v", i, task.Status)
		}
		if task.ID == uuid.Nil {
			t.Errorf("task %d: nil UUID", i)
		}
		if task.QueuedAt.IsZero() {
			t.Errorf("task %d: zero QueuedAt time", i)
		}
	}

	// Verify list returns independent copies
	list1 := tq.List()
	list2 := tq.List()
	if len(list1) > 0 {
		list1[0].Status = tempo.TaskStatusRunning
		if list2[0].Status != tempo.TaskStatusWaiting {
			t.Error("modifying list1 affected list2")
		}
	}
}

func TestQueueStatusMethods(t *testing.T) {
	tq := newTestQueue(10)

	// Empty queue tests
	if tq.HasWaiting() {
		t.Error("empty queue should not have waiting tasks")
	}
	if tq.CountStatus(tempo.TaskStatusWaiting) != 0 {
		t.Error("empty queue should have 0 waiting tasks")
	}

	// Add tasks
	for i := 0; i < 5; i++ {
		tq.Add(func(ctx context.Context) {})
	}

	// HasWaiting
	if !tq.HasWaiting() {
		t.Error("queue should have waiting tasks")
	}

	// CountStatus
	if count := tq.CountStatus(tempo.TaskStatusWaiting); count != 5 {
		t.Errorf("expected 5 waiting tasks, got %d", count)
	}
	if count := tq.CountStatus(tempo.TaskStatusRunning); count != 0 {
		t.Errorf("expected 0 running tasks, got %d", count)
	}
}

func TestQueueSetStatus(t *testing.T) {
	tq := newTestQueue(10)

	id, _ := tq.Add(func(ctx context.Context) {})

	// Change status
	statusTransitions := []tempo.TaskStatus{
		tempo.TaskStatusRunning,
		tempo.TaskStatusComplete,
		tempo.TaskStatusWaiting,
	}

	for _, status := range statusTransitions {
		tq.SetStatus(id, status)
		tasks := tq.List()
		if tasks[0].Status != status {
			t.Errorf("expected status %v, got %v", status, tasks[0].Status)
		}
	}
}

func TestQueueStartTask(t *testing.T) {
	t.Run("basic start waiting", func(t *testing.T) {
		tq := newTestQueue(10)

		// Empty queue
		if _, err := tq.StartTask(); !errors.Is(err, tempo.ErrTaskNotFound) {
			t.Errorf("expected ErrTaskNotFound, got: %v", err)
		}

		// Add and start task
		id, _ := tq.Add(func(ctx context.Context) {})
		task, err := tq.StartTask()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if task.ID != id {
			t.Errorf("expected ID %v, got %v", id, task.ID)
		}
		if task.Status != tempo.TaskStatusRunning {
			t.Errorf("expected running status, got %v", task.Status)
		}

		// Verify status in queue
		if tq.CountStatus(tempo.TaskStatusRunning) != 1 {
			t.Error("expected 1 running task")
		}
		if tq.CountStatus(tempo.TaskStatusWaiting) != 0 {
			t.Error("expected 0 waiting tasks")
		}
	})

	t.Run("start multiple tasks", func(t *testing.T) {
		tq := newTestQueue(10)
		numTasks := 5

		// Add tasks
		for i := 0; i < numTasks; i++ {
			tq.Add(func(ctx context.Context) {})
		}

		// Start all
		startedIDs := make(map[uuid.UUID]bool)
		for i := 0; i < numTasks; i++ {
			task, err := tq.StartTask()
			if err != nil {
				t.Fatalf("error starting task %d: %v", i, err)
			}
			if startedIDs[task.ID] {
				t.Errorf("duplicate task started: %v", task.ID)
			}
			startedIDs[task.ID] = true
		}

		// All should be running
		if tq.CountStatus(tempo.TaskStatusRunning) != numTasks {
			t.Errorf("expected %d running tasks, got %d", numTasks, tq.CountStatus(tempo.TaskStatusRunning))
		}

		// Next start should fail
		if _, err := tq.StartTask(); !errors.Is(err, tempo.ErrTaskNotFound) {
			t.Errorf("expected ErrTaskNotFound, got: %v", err)
		}
	})

	t.Run("start only affects waiting tasks", func(t *testing.T) {
		tq := newTestQueue(10)

		id1, _ := tq.Add(func(ctx context.Context) {})
		id2, _ := tq.Add(func(ctx context.Context) {})
		id3, _ := tq.Add(func(ctx context.Context) {})

		// Set middle task to completed
		tq.SetStatus(id2, tempo.TaskStatusComplete)

		// Start should skip completed task
		task, _ := tq.StartTask()
		if task.ID != id1 {
			t.Errorf("expected first task %v, got %v", id1, task.ID)
		}

		task, _ = tq.StartTask()
		if task.ID != id3 {
			t.Errorf("expected third task %v, got %v", id3, task.ID)
		}

		// Verify id2 still completed
		tasks := tq.List()
		for _, task := range tasks {
			if task.ID == id2 && task.Status != tempo.TaskStatusComplete {
				t.Errorf("task %v status changed unexpectedly", id2)
			}
		}
	})
}

func TestQueueWaitForTask(t *testing.T) {
	t.Run("immediate return when task waiting", func(t *testing.T) {
		tq := newTestQueue(10)
		_, err := tq.Add(func(ctx context.Context) {})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// should not block the execution
		if err := tq.WaitForTask(context.Background()); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("blocks until task added", func(t *testing.T) {
		tq := newTestQueue(10)

		done := make(chan error, 1)
		go func() {
			done <- tq.WaitForTask(context.Background())
		}()

		// Should be blocking
		select {
		case <-done:
			t.Fatal("WaitForTask returned immediately")
		case <-time.After(20 * time.Millisecond):
			// Good, it's blocking
		}

		// Add task to unblock
		_, err := tq.Add(func(ctx context.Context) {})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		select {
		case err := <-done:
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("WaitForTask did not unblock")
		}
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		tq := newTestQueue(10)
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		if err := tq.WaitForTask(ctx); !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got: %v", err)
		}
	})
}

func TestQueueUnlock(t *testing.T) {
	tq := newTestQueue(10)

	// Multiple unlocks should not panic
	tq.Unlock()
	tq.Unlock()

	// Queue should still work
	if _, err := tq.Add(func(ctx context.Context) {}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Unlock with waiting goroutine
	done := make(chan error, 1)
	go func() {
		done <- tq.WaitForTask(context.Background())
	}()

	time.Sleep(10 * time.Millisecond)
	tq.Add(func(ctx context.Context) {})
	tq.Unlock()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("WaitForTask did not unblock")
	}
}

func TestQueueConcurrency(t *testing.T) {
	t.Run("concurrent operations", func(t *testing.T) {
		tq := newTestQueue(200)

		// Initial tasks
		for i := 0; i < 10; i++ {
			_, err := tq.Add(func(ctx context.Context) {})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		}

		var wg sync.WaitGroup
		// Mix of operations
		for i := 0; i < 5; i++ {
			wg.Go(func() {
				_, err := tq.Add(func(ctx context.Context) {})
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			})
			wg.Go(func() {
				_ = tq.List()
			})
			wg.Go(func() {
				_ = tq.HasWaiting()
			})
			wg.Go(func() {
				_ = tq.CountStatus(tempo.TaskStatusWaiting)
			})
		}

		wg.Wait()

		// Queue should still be consistent
		if len(tq.List()) < 10 {
			t.Errorf("expected at least 10 tasks, got %d", len(tq.List()))
		}
	})

	t.Run("concurrent start waiting", func(t *testing.T) {
		tq := newTestQueue(100)
		numTasks := 50

		// Add tasks
		for i := 0; i < numTasks; i++ {
			tq.Add(func(ctx context.Context) {})
		}

		// Start concurrently
		var wg sync.WaitGroup
		startedIDs := sync.Map{}
		errs := make(chan error, numTasks)

		for i := 0; i < numTasks; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				task, err := tq.StartTask()
				if err != nil {
					errs <- err
					return
				}
				if _, exists := startedIDs.LoadOrStore(task.ID, true); exists {
					errs <- fmt.Errorf("duplicate task: %v", task.ID)
				}
			}()
		}

		wg.Wait()
		close(errs)

		for err := range errs {
			t.Errorf("concurrent error: %v", err)
		}

		if tq.CountStatus(tempo.TaskStatusRunning) != numTasks {
			t.Errorf("expected %d running, got %d", numTasks, tq.CountStatus(tempo.TaskStatusRunning))
		}
	})

	t.Run("concurrent set status", func(t *testing.T) {
		tq := newTestQueue(100)

		// Add tasks
		ids := make([]uuid.UUID, 10)
		for i := range ids {
			ids[i], _ = tq.Add(func(ctx context.Context) {})
		}

		var wg sync.WaitGroup
		for i, id := range ids {
			wg.Add(1)
			go func(taskID uuid.UUID, idx int) {
				defer wg.Done()
				if idx%2 == 0 {
					tq.SetStatus(taskID, tempo.TaskStatusRunning)
				} else {
					tq.SetStatus(taskID, tempo.TaskStatusComplete)
				}
			}(id, i)
		}

		wg.Wait()

		// Verify statuses updated
		tasks := tq.List()
		for i, task := range tasks {
			expectedStatus := tempo.TaskStatusRunning
			if i%2 != 0 {
				expectedStatus = tempo.TaskStatusComplete
			}
			if task.Status != expectedStatus {
				t.Errorf("task %d: expected %v, got %v", i, expectedStatus, task.Status)
			}
		}
	})
}

func TestQueueEdgeCases(t *testing.T) {
	t.Run("nil function", func(t *testing.T) {
		tq := newTestQueue(10)
		id, err := tq.Add(nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if id == uuid.Nil {
			t.Error("expected non-nil UUID for nil function")
		}
	})

	t.Run("operations consistency", func(t *testing.T) {
		tq := newTestQueue(10)
		numTasks := 5

		for i := 0; i < numTasks; i++ {
			tq.Add(func(ctx context.Context) {})
		}

		list := tq.List()
		count := tq.CountStatus(tempo.TaskStatusWaiting)
		hasWaiting := tq.HasWaiting()

		if len(list) != numTasks {
			t.Errorf("List: expected %d, got %d", numTasks, len(list))
		}
		if count != numTasks {
			t.Errorf("CountStatus: expected %d, got %d", numTasks, count)
		}
		if !hasWaiting {
			t.Error("HasWaiting: expected true")
		}
	})

	t.Run("capacity limits", func(t *testing.T) {
		capacities := []int{1, 5, 15}
		for _, cap := range capacities {
			t.Run(fmt.Sprintf("capacity_%d", cap), func(t *testing.T) {
				tq := newTestQueue(cap)

				// Fill to capacity
				for i := 0; i < cap; i++ {
					if _, err := tq.Add(func(ctx context.Context) {}); err != nil {
						t.Fatalf("error at task %d: %v", i, err)
					}
				}

				// Next should fail
				if _, err := tq.Add(func(ctx context.Context) {}); !errors.Is(err, tempo.ErrQueueFull) {
					t.Errorf("expected ErrQueueFull, got: %v", err)
				}
			})
		}
	})
}

func TestQueueIntegration(t *testing.T) {
	t.Run("complete workflow", func(t *testing.T) {
		tq := newTestQueue(20)

		// Add tasks
		addedIDs := make([]uuid.UUID, 10)
		for i := range addedIDs {
			addedIDs[i], _ = tq.Add(func(ctx context.Context) {})
		}

		// Verify initial state
		if !tq.HasWaiting() {
			t.Error("expected waiting tasks")
		}
		if tq.CountStatus(tempo.TaskStatusWaiting) != 10 {
			t.Errorf("expected 10 waiting tasks, got %d", tq.CountStatus(tempo.TaskStatusWaiting))
		}

		// List tasks
		tasks := tq.List()
		if len(tasks) != 10 {
			t.Errorf("expected 10 tasks in list, got %d", len(tasks))
		}

		// Verify IDs
		taskIDs := make(map[uuid.UUID]bool)
		for _, task := range tasks {
			taskIDs[task.ID] = true
		}
		for i, id := range addedIDs {
			if !taskIDs[id] {
				t.Errorf("task %d ID %v not found", i, id)
			}
		}

	})

	t.Run("status transitions", func(t *testing.T) {
		tq := newTestQueue(10)

		// Add tasks
		for i := 0; i < 5; i++ {
			tq.Add(func(ctx context.Context) {})
		}

		// Start first task
		task1, _ := tq.StartTask()
		if task1.Status != tempo.TaskStatusRunning {
			t.Errorf("expected running, got %v", task1.Status)
		}

		// Complete it
		tq.SetStatus(task1.ID, tempo.TaskStatusComplete)

		// Verify counts
		want := map[tempo.TaskStatus]int{
			tempo.TaskStatusWaiting:  4,
			tempo.TaskStatusRunning:  0,
			tempo.TaskStatusComplete: 1,
		}

		for status, count := range want {
			if got := tq.CountStatus(status); got != count {
				t.Errorf("status %v: expected %d, got %d", status, count, got)
			}
		}

		// Complete remaining tasks
		for i := 0; i < 4; i++ {
			task, _ := tq.StartTask()
			tq.SetStatus(task.ID, tempo.TaskStatusComplete)
		}

		if tq.CountStatus(tempo.TaskStatusComplete) != 5 {
			t.Errorf("expected 5 completed, got %d", tq.CountStatus(tempo.TaskStatusComplete))
		}
	})

	t.Run("fill to capacity", func(t *testing.T) {
		capacity := 15
		tq := newTestQueue(capacity)

		for i := 0; i < capacity; i++ {
			if _, err := tq.Add(func(ctx context.Context) {}); err != nil {
				t.Fatalf("error at task %d: %v", i, err)
			}
		}

		tasks := tq.List()
		if diff := cmp.Diff(len(tasks), capacity); diff != "" {
			t.Errorf("unexpected count (-got +want)\n%s", diff)
		}

		if _, err := tq.Add(func(ctx context.Context) {}); err == nil {
			t.Error("expected error when adding to full queue")
		}
	})
}
