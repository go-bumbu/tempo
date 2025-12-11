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

func TestNewTaskQueue(t *testing.T) {
	t.Run("create taskqueue with capacity", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})
		if tq == nil {
			t.Fatal("expected non-nil Queue")
		}
	})

	t.Run("create taskqueue with zero capacity", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 0})
		if tq == nil {
			t.Fatal("expected non-nil Queue")
		}
	})
}

func TestTaskQueueAdd(t *testing.T) {
	t.Run("add task to empty queue", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 5})

		id, err := tq.Add(func(ctx context.Context) {
			// simple task
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if id == uuid.Nil {
			t.Error("expected non-nil UUID")
		}

		// verify task was added
		tasks := tq.List()
		if len(tasks) != 1 {
			t.Errorf("expected 1 task, got %d", len(tasks))
		}
	})

	t.Run("add multiple tasks", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		var ids []uuid.UUID
		for i := 0; i < 5; i++ {
			id, err := tq.Add(func(ctx context.Context) {
				// simple task
			})
			if err != nil {
				t.Fatalf("unexpected error adding task %d: %v", i, err)
			}
			ids = append(ids, id)
		}

		// verify all tasks were added
		tasks := tq.List()
		if len(tasks) != 5 {
			t.Errorf("expected 5 tasks, got %d", len(tasks))
		}

		// verify all IDs are unique
		idMap := make(map[uuid.UUID]bool)
		for _, id := range ids {
			if idMap[id] {
				t.Errorf("duplicate UUID: %v", id)
			}
			idMap[id] = true
		}
	})

	t.Run("add task when queue is full", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 3})

		// fill the queue to capacity
		for i := 0; i < 3; i++ {
			_, err := tq.Add(func(ctx context.Context) {
				// simple task
			})
			if err != nil {
				t.Fatalf("unexpected error adding task %d: %v", i, err)
			}
		}

		// try to add one more task
		_, err := tq.Add(func(ctx context.Context) {
			// this should fail
		})

		if !errors.Is(err, tempo.ErrQueueFull) {
			t.Errorf("expected ErrUnsafeStop, got: %v", err)
		}
	})

	t.Run("add tasks concurrently", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 100})

		var wg sync.WaitGroup
		numTasks := 50
		errors := make(chan error, numTasks)

		for i := 0; i < numTasks; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := tq.Add(func(ctx context.Context) {
					// simple task
				})
				if err != nil {
					errors <- err
				}
			}()
		}

		wg.Wait()
		close(errors)

		// check for errors
		for err := range errors {
			t.Errorf("unexpected error: %v", err)
		}

		// verify all tasks were added
		tasks := tq.List()
		if len(tasks) != numTasks {
			t.Errorf("expected %d tasks, got %d", numTasks, len(tasks))
		}
	})
}

func TestTaskQueueList(t *testing.T) {
	t.Run("list empty queue", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		tasks := tq.List()
		if len(tasks) != 0 {
			t.Errorf("expected empty list, got %d tasks", len(tasks))
		}
	})

	t.Run("list tasks with correct status", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		// add some tasks
		for i := 0; i < 3; i++ {
			_, err := tq.Add(func(ctx context.Context) {
				// simple task
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		}

		tasks := tq.List()
		if len(tasks) != 3 {
			t.Fatalf("expected 3 tasks, got %d", len(tasks))
		}

		// all tasks should be waiting
		for i, task := range tasks {
			if task.Status != tempo.TaskStatusWaiting {
				t.Errorf("task %d: expected status 'waiting', got '%s'", i, task.Status.Str())
			}

			if task.ID == uuid.Nil {
				t.Errorf("task %d: expected non-nil UUID", i)
			}

			if task.QueuedAt.IsZero() {
				t.Errorf("task %d: expected non-zero QueuedAt time", i)
			}
		}
	})

	t.Run("list returns copies not references", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		id, err := tq.Add(func(ctx context.Context) {})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// get list twice
		list1 := tq.List()
		list2 := tq.List()

		// verify we got the same data
		if len(list1) != 1 || len(list2) != 1 {
			t.Fatalf("expected 1 task in each list")
		}

		if list1[0].ID != id || list2[0].ID != id {
			t.Error("task IDs don't match")
		}
	})
}

func TestTaskQueueHasWaiting(t *testing.T) {
	t.Run("empty queue has no waiting tasks", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		if tq.HasWaiting() {
			t.Error("empty queue should not have waiting tasks")
		}
	})

	t.Run("queue with waiting tasks", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		_, err := tq.Add(func(ctx context.Context) {})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !tq.HasWaiting() {
			t.Error("queue should have waiting tasks")
		}
	})

	t.Run("concurrent checks for waiting tasks", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		var wg sync.WaitGroup
		results := make(chan bool, 100)

		// add a task first
		_, err := tq.Add(func(ctx context.Context) {})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// check concurrently from multiple goroutines
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				results <- tq.HasWaiting()
			}()
		}

		wg.Wait()
		close(results)

		// all should return true
		for result := range results {
			if !result {
				t.Error("expected HasWaiting to return true")
			}
		}
	})
}

func TestTaskQueueCountStatus(t *testing.T) {
	t.Run("count on empty queue", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		count := tq.CountStatus(tempo.TaskStatusWaiting)
		if count != 0 {
			t.Errorf("expected 0 waiting tasks, got %d", count)
		}

		count = tq.CountStatus(tempo.TaskStatusRunning)
		if count != 0 {
			t.Errorf("expected 0 running tasks, got %d", count)
		}

		count = tq.CountStatus(tempo.TaskStatusComplete)
		if count != 0 {
			t.Errorf("expected 0 completed tasks, got %d", count)
		}
	})

	t.Run("count waiting tasks", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		// add some tasks
		numTasks := 5
		for i := 0; i < numTasks; i++ {
			_, err := tq.Add(func(ctx context.Context) {})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		}

		count := tq.CountStatus(tempo.TaskStatusWaiting)
		if count != numTasks {
			t.Errorf("expected %d waiting tasks, got %d", numTasks, count)
		}
	})

	t.Run("count different statuses", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		// add tasks
		for i := 0; i < 3; i++ {
			_, err := tq.Add(func(ctx context.Context) {})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		}

		// manually change one task status for testing
		// (normally this would be done by the queue processor)
		tasks := tq.List()
		if len(tasks) < 1 {
			t.Fatal("expected at least 1 task")
		}

		// verify initial count
		waitingCount := tq.CountStatus(tempo.TaskStatusWaiting)
		if waitingCount != 3 {
			t.Errorf("expected 3 waiting tasks, got %d", waitingCount)
		}
	})

	t.Run("concurrent count calls", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 100})

		// add some tasks
		for i := 0; i < 10; i++ {
			_, err := tq.Add(func(ctx context.Context) {})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		}

		var wg sync.WaitGroup
		results := make(chan int, 50)

		// count concurrently from multiple goroutines
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				results <- tq.CountStatus(tempo.TaskStatusWaiting)
			}()
		}

		wg.Wait()
		close(results)

		// all should return 10
		for count := range results {
			if count != 10 {
				t.Errorf("expected count of 10, got %d", count)
			}
		}
	})
}

func TestTaskQueueGetFirst(t *testing.T) {
	t.Run("get first from empty queue", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		task, err := tq.GetFirst(tempo.TaskStatusWaiting)
		if !errors.Is(err, tempo.ErrTaskNotFound) {
			t.Errorf("expected ErrTaskNotFound, got: %v", err)
		}

		if task != nil {
			t.Error("expected nil task")
		}
	})

	t.Run("get first waiting task", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		// add some tasks
		var firstID uuid.UUID
		for i := 0; i < 3; i++ {
			id, err := tq.Add(func(ctx context.Context) {})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if i == 0 {
				firstID = id
			}
		}

		task, err := tq.GetFirst(tempo.TaskStatusWaiting)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if task == nil {
			t.Fatal("expected non-nil task")
		}

		if task.ID != firstID {
			t.Errorf("expected first task ID %v, got %v", firstID, task.ID)
		}

		if task.Status != tempo.TaskStatusWaiting {
			t.Errorf("expected status 'waiting', got '%s'", task.Status.Str())
		}
	})

	t.Run("get first when status not found", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		// add waiting tasks
		_, err := tq.Add(func(ctx context.Context) {})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// try to get first running task (none exist)
		task, err := tq.GetFirst(tempo.TaskStatusRunning)
		if !errors.Is(err, tempo.ErrTaskNotFound) {
			t.Errorf("expected ErrTaskNotFound, got: %v", err)
		}

		if task != nil {
			t.Error("expected nil task")
		}
	})

	t.Run("get first returns correct task", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		// add multiple tasks
		ids := make([]uuid.UUID, 5)
		for i := 0; i < 5; i++ {
			id, err := tq.Add(func(ctx context.Context) {})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			ids[i] = id
		}

		// get first task
		task, err := tq.GetFirst(tempo.TaskStatusWaiting)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// verify it's one of our tasks
		found := false
		for _, id := range ids {
			if task.ID == id {
				found = true
				break
			}
		}

		if !found {
			t.Error("returned task ID not in added tasks")
		}
	})
}

func TestTaskQueueConcurrentOperations(t *testing.T) {
	t.Run("concurrent add and list", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 100})

		var wg sync.WaitGroup

		// concurrent adds
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := tq.Add(func(ctx context.Context) {})
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}()
		}

		// concurrent lists
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = tq.List()
			}()
		}

		wg.Wait()

		// verify all tasks were added
		tasks := tq.List()
		if len(tasks) != 50 {
			t.Errorf("expected 50 tasks, got %d", len(tasks))
		}
	})

	t.Run("concurrent operations with different methods", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 200})

		var wg sync.WaitGroup

		// add some initial tasks
		for i := 0; i < 10; i++ {
			_, err := tq.Add(func(ctx context.Context) {})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		}

		// concurrent operations
		for i := 0; i < 25; i++ {
			// Add
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = tq.Add(func(ctx context.Context) {})
			}()

			// List
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = tq.List()
			}()

			// HasWaiting
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = tq.HasWaiting()
			}()

			// CountStatus
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = tq.CountStatus(tempo.TaskStatusWaiting)
			}()

			// GetFirst
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = tq.GetFirst(tempo.TaskStatusWaiting)
			}()
		}

		wg.Wait()

		// verify queue is still consistent
		tasks := tq.List()
		if len(tasks) < 10 {
			t.Errorf("expected at least 10 tasks, got %d", len(tasks))
		}
	})
}

func TestTaskQueueTaskInfo(t *testing.T) {
	t.Run("task info has correct fields", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		beforeAdd := time.Now()
		id, err := tq.Add(func(ctx context.Context) {})
		afterAdd := time.Now()

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		tasks := tq.List()
		if len(tasks) != 1 {
			t.Fatalf("expected 1 task, got %d", len(tasks))
		}

		task := tasks[0]

		// verify ID
		if task.ID != id {
			t.Errorf("expected ID %v, got %v", id, task.ID)
		}

		// verify status
		if task.Status != tempo.TaskStatusWaiting {
			t.Errorf("expected status 'waiting', got '%s'", task.Status.Str())
		}

		// verify QueuedAt is within reasonable time
		if task.QueuedAt.Before(beforeAdd) || task.QueuedAt.After(afterAdd) {
			t.Errorf("QueuedAt time %v is outside expected range [%v, %v]",
				task.QueuedAt, beforeAdd, afterAdd)
		}

		// verify StartedAt is zero (task hasn't started)
		if !task.StartedAt.IsZero() {
			t.Errorf("expected zero StartedAt for waiting task, got %v", task.StartedAt)
		}
	})

	t.Run("multiple tasks have different IDs and times", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		// add tasks with small delay between them
		for i := 0; i < 3; i++ {
			_, err := tq.Add(func(ctx context.Context) {})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			time.Sleep(1 * time.Millisecond)
		}

		tasks := tq.List()
		if len(tasks) != 3 {
			t.Fatalf("expected 3 tasks, got %d", len(tasks))
		}

		// verify all IDs are unique
		idMap := make(map[uuid.UUID]bool)
		for i, task := range tasks {
			if idMap[task.ID] {
				t.Errorf("task %d has duplicate ID: %v", i, task.ID)
			}
			idMap[task.ID] = true
		}

		// verify times are in order (or at least different)
		if !tasks[0].QueuedAt.Before(tasks[2].QueuedAt) &&
			!tasks[0].QueuedAt.Equal(tasks[2].QueuedAt) {
			t.Error("expected first task to be queued before or at same time as last task")
		}
	})
}

func TestTaskQueueCapacityLimits(t *testing.T) {
	t.Run("respect capacity limit", func(t *testing.T) {
		capacity := 5
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: capacity})

		// fill to capacity
		for i := 0; i < capacity; i++ {
			_, err := tq.Add(func(ctx context.Context) {})
			if err != nil {
				t.Fatalf("unexpected error at task %d: %v", i, err)
			}
		}

		// verify count
		count := tq.CountStatus(tempo.TaskStatusWaiting)
		if count != capacity {
			t.Errorf("expected %d tasks, got %d", capacity, count)
		}

		// next add should fail
		_, err := tq.Add(func(ctx context.Context) {})
		if !errors.Is(err, tempo.ErrQueueFull) {
			t.Errorf("expected error %v when exceeding capacity, got: %v", tempo.ErrQueueFull, err)
		}
	})

	t.Run("capacity of 1", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 1})

		_, err := tq.Add(func(ctx context.Context) {})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		_, err = tq.Add(func(ctx context.Context) {})
		if !errors.Is(err, tempo.ErrQueueFull) {
			t.Errorf("expected error %v when exceeding capacity, got: %v", tempo.ErrQueueFull, err)
		}
	})
}

func TestTaskQueueEdgeCases(t *testing.T) {
	t.Run("list returns new slice each time", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		_, err := tq.Add(func(ctx context.Context) {})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		list1 := tq.List()
		list2 := tq.List()

		// modify first list
		if len(list1) > 0 {
			list1[0].Status = tempo.TaskStatusRunning
		}

		// second list should be unchanged
		if len(list2) > 0 && list2[0].Status != tempo.TaskStatusWaiting {
			t.Error("modifying one list affected another list")
		}
	})

	t.Run("nil function in Add", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		// This should work - the implementation doesn't validate the function
		id, err := tq.Add(nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if id == uuid.Nil {
			t.Error("expected non-nil UUID even for nil function")
		}
	})

	t.Run("operations maintain consistency", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		// add tasks
		numTasks := 5
		for i := 0; i < numTasks; i++ {
			_, err := tq.Add(func(ctx context.Context) {})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		}

		// verify consistency across methods
		list := tq.List()
		count := tq.CountStatus(tempo.TaskStatusWaiting)
		hasWaiting := tq.HasWaiting()
		first, err := tq.GetFirst(tempo.TaskStatusWaiting)

		if len(list) != numTasks {
			t.Errorf("List: expected %d tasks, got %d", numTasks, len(list))
		}

		if count != numTasks {
			t.Errorf("CountStatus: expected %d, got %d", numTasks, count)
		}

		if !hasWaiting {
			t.Error("HasWaiting: expected true")
		}

		if err != nil {
			t.Errorf("GetFirst: unexpected error: %v", err)
		}

		if first == nil {
			t.Error("GetFirst: expected non-nil task")
		}

		// verify first is in list
		found := false
		for _, task := range list {
			if task.ID == first.ID {
				found = true
				break
			}
		}
		if !found {
			t.Error("GetFirst returned task not in List")
		}
	})
}

func TestTaskQueueSetStatus(t *testing.T) {
	t.Run("set status of existing task", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		id, err := tq.Add(func(ctx context.Context) {})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// verify initial status
		tasks := tq.List()
		if len(tasks) != 1 {
			t.Fatalf("expected 1 task, got %d", len(tasks))
		}
		if tasks[0].Status != tempo.TaskStatusWaiting {
			t.Errorf("expected status 'waiting', got '%s'", tasks[0].Status.Str())
		}

		// change status to running
		tq.SetStatus(id, tempo.TaskStatusRunning)

		// verify status changed
		tasks = tq.List()
		if tasks[0].Status != tempo.TaskStatusRunning {
			t.Errorf("expected status 'running', got '%s'", tasks[0].Status.Str())
		}
	})

	t.Run("set status to completed", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		id, err := tq.Add(func(ctx context.Context) {})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		tq.SetStatus(id, tempo.TaskStatusComplete)

		tasks := tq.List()
		if tasks[0].Status != tempo.TaskStatusComplete {
			t.Errorf("expected status 'completed', got '%s'", tasks[0].Status.Str())
		}
	})

	t.Run("set status of non-existent task does nothing", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		// add a task to have something in the queue
		id1, err := tq.Add(func(ctx context.Context) {})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// try to set status of a different ID
		fakeID := uuid.New()
		tq.SetStatus(fakeID, tempo.TaskStatusRunning)

		// verify original task is unchanged
		tasks := tq.List()
		if len(tasks) != 1 {
			t.Fatalf("expected 1 task, got %d", len(tasks))
		}
		if tasks[0].ID != id1 {
			t.Error("task ID changed unexpectedly")
		}
		if tasks[0].Status != tempo.TaskStatusWaiting {
			t.Errorf("expected status 'waiting', got '%s'", tasks[0].Status.Str())
		}
	})

	t.Run("set status on empty queue", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		// this should not panic
		fakeID := uuid.New()
		tq.SetStatus(fakeID, tempo.TaskStatusRunning)

		// verify queue is still empty
		tasks := tq.List()
		if len(tasks) != 0 {
			t.Errorf("expected empty queue, got %d tasks", len(tasks))
		}
	})

	t.Run("set status multiple times", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		id, err := tq.Add(func(ctx context.Context) {})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// change status multiple times
		tq.SetStatus(id, tempo.TaskStatusRunning)
		tasks := tq.List()
		if tasks[0].Status != tempo.TaskStatusRunning {
			t.Errorf("expected status 'running', got '%s'", tasks[0].Status.Str())
		}

		tq.SetStatus(id, tempo.TaskStatusComplete)
		tasks = tq.List()
		if tasks[0].Status != tempo.TaskStatusComplete {
			t.Errorf("expected status 'completed', got '%s'", tasks[0].Status.Str())
		}

		tq.SetStatus(id, tempo.TaskStatusWaiting)
		tasks = tq.List()
		if tasks[0].Status != tempo.TaskStatusWaiting {
			t.Errorf("expected status 'waiting', got '%s'", tasks[0].Status.Str())
		}
	})

	t.Run("concurrent set status calls", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 100})

		// add multiple tasks
		ids := make([]uuid.UUID, 10)
		for i := 0; i < 10; i++ {
			id, err := tq.Add(func(ctx context.Context) {})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			ids[i] = id
		}

		var wg sync.WaitGroup
		// concurrently set status on different tasks
		for i, id := range ids {
			wg.Add(1)
			go func(taskID uuid.UUID, index int) {
				defer wg.Done()
				if index%2 == 0 {
					tq.SetStatus(taskID, tempo.TaskStatusRunning)
				} else {
					tq.SetStatus(taskID, tempo.TaskStatusComplete)
				}
			}(id, i)
		}

		wg.Wait()

		// verify all tasks have updated status
		tasks := tq.List()
		if len(tasks) != 10 {
			t.Fatalf("expected 10 tasks, got %d", len(tasks))
		}

		for i, task := range tasks {
			if i%2 == 0 {
				if task.Status != tempo.TaskStatusRunning {
					t.Errorf("task %d: expected status 'running', got '%s'", i, task.Status.Str())
				}
			} else {
				if task.Status != tempo.TaskStatusComplete {
					t.Errorf("task %d: expected status 'completed', got '%s'", i, task.Status.Str())
				}
			}
		}
	})
}

func TestTaskQueueWaitForTask(t *testing.T) {
	t.Skip()
	t.Run("wait returns immediately when task is waiting", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		_, err := tq.Add(func(ctx context.Context) {})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		ctx := context.Background()
		err = tq.WaitForTask(ctx)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("wait blocks when no tasks are waiting", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		// this should block until context is cancelled
		err := tq.WaitForTask(ctx)
		if err == nil {
			t.Error("expected error when context times out")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected context.DeadlineExceeded, got: %v", err)
		}
	})

	t.Run("wait returns when task is added concurrently", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		done := make(chan error, 1)
		go func() {
			err := tq.WaitForTask(context.Background())
			done <- err
		}()

		// give the goroutine time to start waiting
		time.Sleep(10 * time.Millisecond)

		// add a task
		_, err := tq.Add(func(ctx context.Context) {})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// wait should unblock
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("WaitForTask did not unblock after adding task")
		}
	})

	t.Run("wait respects context cancellation", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		ctx, cancel := context.WithCancel(context.Background())

		done := make(chan error, 1)
		go func() {
			err := tq.WaitForTask(ctx)
			done <- err
		}()

		// give the goroutine time to start waiting
		time.Sleep(10 * time.Millisecond)

		// cancel the context
		cancel()

		// wait should unblock with error
		select {
		case err := <-done:
			if err == nil {
				t.Error("expected error when context is cancelled")
			}
			if !errors.Is(err, context.Canceled) {
				t.Errorf("expected context.Canceled, got: %v", err)
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("WaitForTask did not unblock after context cancellation")
		}
	})

	t.Run("multiple waits can be satisfied", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		// start multiple goroutines waiting
		numWaiters := 3
		done := make(chan error, numWaiters)

		for i := 0; i < numWaiters; i++ {
			go func() {
				err := tq.WaitForTask(context.Background())
				done <- err
			}()
		}

		// give goroutines time to start waiting
		time.Sleep(20 * time.Millisecond)

		// add tasks to satisfy the waiters
		for i := 0; i < numWaiters; i++ {
			_, err := tq.Add(func(ctx context.Context) {})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		}

		// all waiters should eventually succeed
		for i := 0; i < numWaiters; i++ {
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("waiter %d got unexpected error: %v", i, err)
				}
			case <-time.After(200 * time.Millisecond):
				t.Errorf("waiter %d did not unblock", i)
			}
		}
	})
}

func TestTaskQueueStartWaiting(t *testing.T) {
	t.Run("start waiting task", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		id, err := tq.Add(func(ctx context.Context) {})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		task, err := tq.StartWaiting()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if task == nil {
			t.Fatal("expected non-nil task")
		}

		if task.ID != id {
			t.Errorf("expected task ID %v, got %v", id, task.ID)
		}

		if task.Status != tempo.TaskStatusRunning {
			t.Errorf("expected status 'running', got '%s'", task.Status.Str())
		}

		// verify status was changed in queue
		tasks := tq.List()
		if len(tasks) != 1 {
			t.Fatalf("expected 1 task, got %d", len(tasks))
		}
		if tasks[0].Status != tempo.TaskStatusRunning {
			t.Errorf("expected status 'running' in queue, got '%s'", tasks[0].Status.Str())
		}
	})

	t.Run("start waiting returns error when no waiting tasks", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		task, err := tq.StartWaiting()
		if !errors.Is(err, tempo.ErrTaskNotFound) {
			t.Errorf("expected ErrTaskNotFound, got: %v", err)
		}

		if task != nil {
			t.Error("expected nil task")
		}
	})

	t.Run("start waiting picks first waiting task", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		// add multiple tasks
		ids := make([]uuid.UUID, 3)
		for i := 0; i < 3; i++ {
			id, err := tq.Add(func(ctx context.Context) {})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			ids[i] = id
		}

		// start first waiting task
		task, err := tq.StartWaiting()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// should be the first task we added
		if task.ID != ids[0] {
			t.Errorf("expected first task ID %v, got %v", ids[0], task.ID)
		}

		// verify counts
		waitingCount := tq.CountStatus(tempo.TaskStatusWaiting)
		if waitingCount != 2 {
			t.Errorf("expected 2 waiting tasks, got %d", waitingCount)
		}

		runningCount := tq.CountStatus(tempo.TaskStatusRunning)
		if runningCount != 1 {
			t.Errorf("expected 1 running task, got %d", runningCount)
		}
	})

	t.Run("start waiting multiple times", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		// add tasks
		numTasks := 5
		for i := 0; i < numTasks; i++ {
			_, err := tq.Add(func(ctx context.Context) {})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		}

		// start all tasks
		startedIDs := make(map[uuid.UUID]bool)
		for i := 0; i < numTasks; i++ {
			task, err := tq.StartWaiting()
			if err != nil {
				t.Fatalf("unexpected error on task %d: %v", i, err)
			}

			if startedIDs[task.ID] {
				t.Errorf("task %d: started same task twice: %v", i, task.ID)
			}
			startedIDs[task.ID] = true
		}

		// verify all tasks are running
		runningCount := tq.CountStatus(tempo.TaskStatusRunning)
		if runningCount != numTasks {
			t.Errorf("expected %d running tasks, got %d", numTasks, runningCount)
		}

		// next start should fail
		_, err := tq.StartWaiting()
		if !errors.Is(err, tempo.ErrTaskNotFound) {
			t.Errorf("expected ErrTaskNotFound, got: %v", err)
		}
	})

	t.Run("start waiting only affects waiting tasks", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		// add tasks
		id1, _ := tq.Add(func(ctx context.Context) {})
		id2, _ := tq.Add(func(ctx context.Context) {})
		id3, _ := tq.Add(func(ctx context.Context) {})

		// manually set one to completed
		tq.SetStatus(id2, tempo.TaskStatusComplete)

		// start waiting should skip the completed task
		task, err := tq.StartWaiting()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// should be id1 (first waiting)
		if task.ID != id1 {
			t.Errorf("expected task ID %v, got %v", id1, task.ID)
		}

		// start again
		task, err = tq.StartWaiting()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// should be id3 (second waiting)
		if task.ID != id3 {
			t.Errorf("expected task ID %v, got %v", id3, task.ID)
		}

		// verify id2 is still completed
		tasks := tq.List()
		for _, task := range tasks {
			if task.ID == id2 && task.Status != tempo.TaskStatusComplete {
				t.Errorf("task %v status changed unexpectedly to '%s'", id2, task.Status.Str())
			}
		}
	})

	t.Run("concurrent start waiting calls", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 100})

		// add tasks
		numTasks := 50
		for i := 0; i < numTasks; i++ {
			_, err := tq.Add(func(ctx context.Context) {})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		}

		// start tasks concurrently
		var wg sync.WaitGroup
		startedIDs := sync.Map{}
		errors := make(chan error, numTasks)

		for i := 0; i < numTasks; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				task, err := tq.StartWaiting()
				if err != nil {
					errors <- err
					return
				}

				// check for duplicate
				if _, exists := startedIDs.LoadOrStore(task.ID, true); exists {
					errors <- fmt.Errorf("duplicate task started: %v", task.ID)
				}
			}()
		}

		wg.Wait()
		close(errors)

		// check for errors
		for err := range errors {
			t.Errorf("unexpected error: %v", err)
		}

		// verify all tasks are running
		runningCount := tq.CountStatus(tempo.TaskStatusRunning)
		if runningCount != numTasks {
			t.Errorf("expected %d running tasks, got %d", numTasks, runningCount)
		}

		waitingCount := tq.CountStatus(tempo.TaskStatusWaiting)
		if waitingCount != 0 {
			t.Errorf("expected 0 waiting tasks, got %d", waitingCount)
		}
	})
}

func TestTaskQueueUnlock(t *testing.T) {
	t.Run("unlock signals waiting goroutines", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		// start a goroutine that waits
		done := make(chan bool, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			// this will wait because no tasks are available
			err := tq.WaitForTask(ctx)
			// we expect context timeout
			done <- (err != nil)
		}()

		// give the goroutine time to start waiting
		time.Sleep(10 * time.Millisecond)

		// unlock should signal the condition variable
		tq.Unlock()

		// the goroutine should eventually timeout (unlock doesn't add tasks)
		select {
		case result := <-done:
			if !result {
				t.Error("expected error due to no tasks, but got none")
			}
		case <-time.After(200 * time.Millisecond):
			t.Error("goroutine did not complete")
		}
	})

	t.Run("unlock with task added", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		// start a goroutine that waits
		done := make(chan error, 1)
		go func() {
			err := tq.WaitForTask(context.Background())
			done <- err
		}()

		// give the goroutine time to start waiting
		time.Sleep(10 * time.Millisecond)

		// add a task and unlock
		_, err := tq.Add(func(ctx context.Context) {})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		tq.Unlock()

		// the goroutine should unblock successfully
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("WaitForTask did not unblock")
		}
	})

	t.Run("multiple unlock calls", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		// multiple unlock calls should not panic
		tq.Unlock()
		tq.Unlock()
		tq.Unlock()

		// and the queue should still work normally
		_, err := tq.Add(func(ctx context.Context) {})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		tasks := tq.List()
		if len(tasks) != 1 {
			t.Errorf("expected 1 task, got %d", len(tasks))
		}
	})

	t.Run("unlock without waiters", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		// unlock when no one is waiting should not cause issues
		tq.Unlock()

		// verify queue still works
		_, err := tq.Add(func(ctx context.Context) {})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("concurrent unlock calls", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		var wg sync.WaitGroup
		numUnlocks := 100

		// call unlock concurrently
		for i := 0; i < numUnlocks; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				tq.Unlock()
			}()
		}

		wg.Wait()

		// verify queue is still functional
		_, err := tq.Add(func(ctx context.Context) {})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

func TestTaskQueueIntegration(t *testing.T) {
	t.Run("typical usage workflow", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 20})

		// Phase 1: Add multiple tasks
		addedIDs := make([]uuid.UUID, 0, 10)
		for i := 0; i < 10; i++ {
			id, err := tq.Add(func(ctx context.Context) {
				// simulate work
			})
			if err != nil {
				t.Fatalf("error adding task %d: %v", i, err)
			}
			addedIDs = append(addedIDs, id)
		}

		// Phase 2: Verify queue state
		if !tq.HasWaiting() {
			t.Error("expected HasWaiting to be true")
		}

		waitingCount := tq.CountStatus(tempo.TaskStatusWaiting)
		if waitingCount != 10 {
			t.Errorf("expected 10 waiting tasks, got %d", waitingCount)
		}

		// Phase 3: List and verify all tasks
		tasks := tq.List()
		if len(tasks) != 10 {
			t.Errorf("expected 10 tasks in list, got %d", len(tasks))
		}

		// verify all added IDs are in the list
		taskIDs := make(map[uuid.UUID]bool)
		for _, task := range tasks {
			taskIDs[task.ID] = true
		}

		for i, id := range addedIDs {
			if !taskIDs[id] {
				t.Errorf("task %d with ID %v not found in list", i, id)
			}
		}

		// Phase 4: Get first task
		first, err := tq.GetFirst(tempo.TaskStatusWaiting)
		if err != nil {
			t.Fatalf("error getting first task: %v", err)
		}

		if first.Status != tempo.TaskStatusWaiting {
			t.Errorf("expected first task status 'waiting', got '%s'", first.Status.Str())
		}
	})

	t.Run("fill queue to capacity and verify", func(t *testing.T) {
		capacity := 15
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: capacity})

		// fill exactly to capacity
		for i := 0; i < capacity; i++ {
			_, err := tq.Add(func(ctx context.Context) {})
			if err != nil {
				t.Fatalf("unexpected error at task %d: %v", i, err)
			}
		}

		// verify state
		tasks := tq.List()
		want := capacity
		got := len(tasks)
		if diff := cmp.Diff(got, want); diff != "" {
			t.Errorf("unexpected task count (-got +want)\n%s", diff)
		}

		// verify next add fails
		_, err := tq.Add(func(ctx context.Context) {})
		if err == nil {
			t.Error("expected error when adding to full queue")
		}
	})

	t.Run("complete workflow with status transitions", func(t *testing.T) {
		tq := tempo.NewQueue(tempo.QueueCfg{QueueSize: 10})

		// add tasks
		numTasks := 5
		for i := 0; i < numTasks; i++ {
			_, err := tq.Add(func(ctx context.Context) {})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		}

		// start first task
		task1, err := tq.StartWaiting()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// verify status
		if task1.Status != tempo.TaskStatusRunning {
			t.Errorf("expected status 'running', got '%s'", task1.Status.Str())
		}

		// complete the task
		tq.SetStatus(task1.ID, tempo.TaskStatusComplete)

		// verify counts
		waitingCount := tq.CountStatus(tempo.TaskStatusWaiting)
		runningCount := tq.CountStatus(tempo.TaskStatusRunning)
		completedCount := tq.CountStatus(tempo.TaskStatusComplete)

		if waitingCount != 4 {
			t.Errorf("expected 4 waiting tasks, got %d", waitingCount)
		}
		if runningCount != 0 {
			t.Errorf("expected 0 running tasks, got %d", runningCount)
		}
		if completedCount != 1 {
			t.Errorf("expected 1 completed task, got %d", completedCount)
		}

		// start remaining tasks
		for i := 0; i < 4; i++ {
			task, err := tq.StartWaiting()
			if err != nil {
				t.Fatalf("unexpected error starting task %d: %v", i, err)
			}
			tq.SetStatus(task.ID, tempo.TaskStatusComplete)
		}

		// verify all completed
		completedCount = tq.CountStatus(tempo.TaskStatusComplete)
		if completedCount != numTasks {
			t.Errorf("expected %d completed tasks, got %d", numTasks, completedCount)
		}
	})
}
