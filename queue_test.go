package tempo

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"sync"
	"testing"
	"time"
)

// Helper to create a test TaskQueue
func newTestQueue(size int) *TaskQueue {
	return NewTaskQueue(QueueCfg{QueueSize: size})
}


HERE => improve tests

const myActionName = "myActionName"

func dummyTask(ctx context.Context) error { return nil }

func TestQueueAdd(t *testing.T) {
	t.Run("basic add operations", func(t *testing.T) {
		tq := newTestQueue(10)

		// Add single task
		id, err := tq.Add(dummyTask, myActionName)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if id == uuid.Nil {
			t.Error("expected non-nil UUID")
		}

		// Add multiple tasks with unique IDs
		ids := make(map[uuid.UUID]bool)
		for i := 0; i < 5; i++ {
			id, err := tq.Add(dummyTask, myActionName)
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

	t.Run("TaskQueue full", func(t *testing.T) {
		tq := newTestQueue(3)

		for i := 0; i < 3; i++ {
			if _, err := tq.Add(dummyTask, myActionName); err != nil {
				t.Fatalf("error at task %d: %v", i, err)
			}
		}

		_, err := tq.Add(dummyTask, myActionName)
		if !errors.Is(err, ErrQueueFull) {
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
				_, _ = tq.Add(dummyTask, myActionName)
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

	// Empty TaskQueue
	if len(tq.List()) != 0 {
		t.Error("expected empty list for new TaskQueue")
	}

	// Add tasks and verify
	for i := 0; i < 3; i++ {
		_, _ = tq.Add(dummyTask, myActionName)
	}

	tasks := tq.List()
	if len(tasks) != 3 {
		t.Fatalf("expected 3 tasks, got %d", len(tasks))
	}

	// Verify task info
	for i, task := range tasks {
		if task.Status != TaskStatusWaiting {
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
		list1[0].Status = TaskStatusRunning
		if list2[0].Status != TaskStatusWaiting {
			t.Error("modifying list1 affected list2")
		}
	}
}

func TestQueueUnlock(t *testing.T) {
	tq := newTestQueue(10)

	// Multiple unlocks should not panic
	tq.UnlockAllWaiting()
	tq.UnlockAllWaiting()

	// TaskQueue should still work
	if _, err := tq.Add(dummyTask, myActionName); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Unlock with waiting goroutine
	done := make(chan error, 1)
	go func() {
		_, err := tq.WaitAndClaimTask(context.Background())
		done <- err
	}()

	time.Sleep(10 * time.Millisecond)
	_, _ = tq.Add(dummyTask, myActionName)
	tq.UnlockAllWaiting()

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
			_, err := tq.Add(dummyTask, myActionName)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		}

		var wg sync.WaitGroup
		// Mix of operations
		for i := 0; i < 5; i++ {
			wg.Go(func() {
				_, err := tq.Add(dummyTask, myActionName)
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			})
			wg.Go(func() {
				_ = tq.List()
			})
		}

		wg.Wait()

		// TaskQueue should still be consistent
		if len(tq.List()) < 10 {
			t.Errorf("expected at least 10 tasks, got %d", len(tq.List()))
		}
	})

	t.Run("concurrent set status", func(t *testing.T) {
		tq := newTestQueue(100)

		// Add tasks
		ids := make([]uuid.UUID, 10)
		for i := range ids {
			ids[i], _ = tq.Add(dummyTask, myActionName)
		}

		var wg sync.WaitGroup
		for i, id := range ids {
			wg.Add(1)
			go func(taskID uuid.UUID, idx int) {
				defer wg.Done()
				if idx%2 == 0 {
					tq.SetStatus(taskID, TaskStatusRunning, nil)
				} else {
					tq.SetStatus(taskID, TaskStatusComplete, nil)
				}
			}(id, i)
		}

		wg.Wait()

		// Verify statuses updated
		tasks := tq.List()
		for i, task := range tasks {
			expectedStatus := TaskStatusRunning
			if i%2 != 0 {
				expectedStatus = TaskStatusComplete
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
		id, err := tq.Add(nil, myActionName)
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
			_, _ = tq.Add(dummyTask, myActionName)
		}

		list := tq.List()
		count := tq.CountStatus(TaskStatusWaiting)

		if len(list) != numTasks {
			t.Errorf("List: expected %d, got %d", numTasks, len(list))
		}
		if count != numTasks {
			t.Errorf("CountStatus: expected %d, got %d", numTasks, count)
		}
	})

	t.Run("capacity limits", func(t *testing.T) {
		capacities := []int{1, 5, 15}
		for _, capacity := range capacities {
			t.Run(fmt.Sprintf("capacity_%d", capacity), func(t *testing.T) {
				tq := newTestQueue(capacity)

				// Fill to capacity
				for i := 0; i < capacity; i++ {
					if _, err := tq.Add(dummyTask, myActionName); err != nil {
						t.Fatalf("error at task %d: %v", i, err)
					}
				}

				// Next should fail
				if _, err := tq.Add(dummyTask, myActionName); !errors.Is(err, ErrQueueFull) {
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
			addedIDs[i], _ = tq.Add(dummyTask, myActionName)
		}

		// Verify initial state
		if tq.CountStatus(TaskStatusWaiting) != 10 {
			t.Errorf("expected 10 waiting tasks, got %d", tq.CountStatus(TaskStatusWaiting))
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
				t.Errorf("task %d id %v not found", i, id)
			}
		}

	})

	t.Run("fill to capacity", func(t *testing.T) {
		capacity := 15
		tq := newTestQueue(capacity)

		for i := 0; i < capacity; i++ {
			if _, err := tq.Add(dummyTask, myActionName); err != nil {
				t.Fatalf("error at task %d: %v", i, err)
			}
		}

		tasks := tq.List()
		if diff := cmp.Diff(len(tasks), capacity); diff != "" {
			t.Errorf("unexpected count (-got +want)\n%s", diff)
		}

		if _, err := tq.Add(dummyTask, myActionName); err == nil {
			t.Error("expected error when adding to full TaskQueue")
		}
	})
}
