package tempo

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
)

func newTestQueue(size int) *TaskQueue {
	return NewTaskQueue(TaskQueueCfg{QueueSize: size, HistorySize: 10})
}

const myActionName = "myActionName"

func TestQueueAdd(t *testing.T) {
	ctx := context.Background()
	t.Run("basic add operations", func(t *testing.T) {
		tq := newTestQueue(10)

		id, err := tq.Add(myActionName, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if id == uuid.Nil {
			t.Error("expected non-nil UUID")
		}

		ids := make(map[uuid.UUID]bool)
		for i := 0; i < 5; i++ {
			id, err := tq.Add(myActionName, nil)
			if err != nil {
				t.Fatalf("error adding task %d: %v", i, err)
			}
			if ids[id] {
				t.Errorf("duplicate UUID: %v", id)
			}
			ids[id] = true
		}

		list, _ := tq.List(ctx)
		if len(list) != 6 {
			t.Errorf("expected 6 tasks, got %d", len(list))
		}
	})

	t.Run("TaskQueue full", func(t *testing.T) {
		tq := newTestQueue(3)

		for i := 0; i < 3; i++ {
			if _, err := tq.Add(myActionName, nil); err != nil {
				t.Fatalf("error at task %d: %v", i, err)
			}
		}

		_, err := tq.Add(myActionName, nil)
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
				_, _ = tq.Add(myActionName, nil)
			}()
		}
		wg.Wait()

		list, _ := tq.List(ctx)
		if len(list) != numTasks {
			t.Errorf("expected %d tasks, got %d", numTasks, len(list))
		}
	})
}

func TestQueueList(t *testing.T) {
	ctx := context.Background()
	tq := newTestQueue(10)

	list, err := tq.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 0 {
		t.Error("expected empty list for new TaskQueue")
	}

	for i := 0; i < 3; i++ {
		_, _ = tq.Add(myActionName, nil)
	}

	list, err = tq.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 3 {
		t.Fatalf("expected 3 tasks, got %d", len(list))
	}

	for i, task := range list {
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

	list1, _ := tq.List(ctx)
	list2, _ := tq.List(ctx)
	if len(list1) > 0 {
		list1[0].Status = TaskStatusRunning
		if list2[0].Status != TaskStatusWaiting {
			t.Error("modifying list1 affected list2")
		}
	}
}

func TestQueueGet(t *testing.T) {
	ctx := context.Background()
	tq := newTestQueue(10)

	for i := 0; i < 3; i++ {
		_, _ = tq.Add(myActionName, nil)
	}

	list, _ := tq.List(ctx)
	if len(list) != 3 {
		t.Fatalf("expected 3 tasks, got %d", len(list))
	}

	t.Run("get existing task", func(t *testing.T) {
		info, err := tq.Get(ctx, list[0].ID)
		if err != nil {
			t.Fatalf("unexpected error getting task: %v", err)
		}
		if info.Status != TaskStatusWaiting {
			t.Errorf("expected waiting status, got %v", info.Status)
		}
		if info.ID == uuid.Nil {
			t.Error("unexpected task nil UUID")
		}
		if info.QueuedAt.IsZero() {
			t.Error("zero QueuedAt time")
		}
	})

	t.Run("error on nonexistent task", func(t *testing.T) {
		randomUID := uuid.Must(uuid.NewRandom())
		_, err := tq.Get(ctx, randomUID)
		if !errors.Is(err, ErrTaskNotFound) {
			t.Errorf("expected ErrTaskNotFound, got: %v", err)
		}
	})
}

func countStatus(list []TaskInfo, status TaskStatus) int {
	n := 0
	for _, t := range list {
		if t.Status == status {
			n++
		}
	}
	return n
}

func TestQueueCleanHistoryNoTerminalTasks(t *testing.T) {
	ctx := context.Background()
	tq := NewTaskQueue(TaskQueueCfg{QueueSize: 10, HistorySize: 1})
	_, _ = tq.Add("a", nil)
	_, _ = tq.Add("b", nil)
	_ = tq.CleanHistory(ctx, 1)
	list, _ := tq.List(ctx)
	if len(list) != 2 {
		t.Errorf("expected 2 tasks, got %d", len(list))
	}
}

func TestQueueCleanHistoryLessThanMaxDone(t *testing.T) {
	ctx := context.Background()
	tq := NewTaskQueue(TaskQueueCfg{QueueSize: 10, HistorySize: 3})
	_, _ = tq.Add("a", nil)
	_, _ = tq.Add("b", nil)
	_, _ = tq.Add("c", nil)
	list, _ := tq.List(ctx)
	for _, task := range list {
		if task.Name == "a" {
			_ = tq.SetStatus(ctx, task.ID, TaskStatusComplete, time.Time{}, time.Now())
		}
		if task.Name == "c" {
			_ = tq.SetStatus(ctx, task.ID, TaskStatusPanicked, time.Time{}, time.Now())
		}
	}
	_ = tq.CleanHistory(ctx, 3)
	list, _ = tq.List(ctx)
	if len(list) != 3 {
		t.Errorf("expected 3 tasks, got %d", len(list))
	}
}

func TestQueueCleanHistoryMoreThanMaxDone(t *testing.T) {
	ctx := context.Background()
	tq := NewTaskQueue(TaskQueueCfg{QueueSize: 10, HistorySize: 2})
	for i := 0; i < 5; i++ {
		_, _ = tq.Add(fmt.Sprintf("task-%d", i), nil)
	}
	list, _ := tq.List(ctx)
	for _, task := range list {
		switch task.Name {
		case "task-0", "task-1", "task-4":
			_ = tq.SetStatus(ctx, task.ID, TaskStatusComplete, time.Time{}, time.Now())
		case "task-2":
			_ = tq.SetStatus(ctx, task.ID, TaskStatusPanicked, time.Time{}, time.Now())
		}
	}
	_ = tq.CleanHistory(ctx, 2)

	list, _ = tq.List(ctx)
	doneCount := countStatus(list, TaskStatusComplete) + countStatus(list, TaskStatusPanicked)
	if doneCount != 2 {
		t.Errorf("expected 2 terminal tasks, got %d", doneCount)
	}
	gotOrder := make([]TaskStatus, 0, len(list))
	for _, task := range list {
		gotOrder = append(gotOrder, task.Status)
	}
	want := []TaskStatus{TaskStatusComplete, TaskStatusWaiting, TaskStatusPanicked}
	if diff := cmp.Diff(gotOrder, want); diff != "" {
		t.Errorf("unexpected order (-got +want)\n%s", diff)
	}
}

func TestQueueCleanHistoryMixPreservesOrder(t *testing.T) {
	ctx := context.Background()
	tq := NewTaskQueue(TaskQueueCfg{QueueSize: 10, HistorySize: 2})
	for _, name := range []string{"a", "b", "c", "d", "e", "f"} {
		_, _ = tq.Add(name, nil)
	}
	list, _ := tq.List(ctx)
	for _, task := range list {
		switch task.Name {
		case "a":
			_ = tq.SetStatus(ctx, task.ID, TaskStatusComplete, time.Time{}, time.Now())
		case "c":
			_ = tq.SetStatus(ctx, task.ID, TaskStatusFailed, time.Time{}, time.Now())
		case "e":
			_ = tq.SetStatus(ctx, task.ID, TaskStatusPanicked, time.Time{}, time.Now())
		}
	}
	_ = tq.CleanHistory(ctx, 2)

	list, _ = tq.List(ctx)
	doneCount := 0
	for _, task := range list {
		if slices.Contains(TaskTerminalStatus, task.Status) {
			doneCount++
		}
	}
	if doneCount != 2 {
		t.Errorf("expected 2 terminal tasks, got %d", doneCount)
	}
	gotOrder := make([]TaskStatus, 0, len(list))
	for _, task := range list {
		gotOrder = append(gotOrder, task.Status)
	}
	want := []TaskStatus{TaskStatusWaiting, TaskStatusPanicked, TaskStatusWaiting, TaskStatusFailed, TaskStatusWaiting}
	if diff := cmp.Diff(gotOrder, want); diff != "" {
		t.Errorf("unexpected order (-got +want)\n%s", diff)
	}
}

func TestQueueUnblock(t *testing.T) {
	ctx := context.Background()
	tq := newTestQueue(10)

	tq.UnblockAll()
	tq.UnblockAll()

	if _, err := tq.Add(myActionName, nil); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, _, _, err := tq.NextTask(ctx, nil)
		done <- err
	}()

	time.Sleep(10 * time.Millisecond)
	_, _ = tq.Add(myActionName, nil)
	tq.UnblockAll()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("NextTask did not unblock")
	}
}

// TestQueueNextTaskUnblocksOnContextCancel documents the contract NextTask
// states in its own doc comment: it "Blocks until an eligible task exists or
// ctx is done". A caller that cancels the context while NextTask is parked in
// cond.Wait therefore expects it to return promptly with ctx.Err().
//
// It currently does not: cancelling the context never signals the cond, so the
// blocked call is only ever woken by an unrelated Add (Signal) or UnblockAll
// (Broadcast). With neither, NextTask is stuck forever — a missed-wakeup that
// leaks the goroutine. The runner hides this by wiring ctx.Done to UnblockAll,
// but TaskQueue is exported and usable on its own, where nothing does that.
//
// EXPECTED TO FAIL until NextTask selects on ctx while waiting.
func TestQueueNextTaskUnblocksOnContextCancel(t *testing.T) {
	tq := newTestQueue(10)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		_, _, _, err := tq.NextTask(ctx, nil)
		done <- err
	}()

	// Let the goroutine reach cond.Wait. The queue is empty, so once it has
	// scanned and found nothing it parks there until something signals it.
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		// Release the parked goroutine so it does not linger for the rest of the
		// suite: after this Broadcast it re-scans, sees ctx is done, and returns.
		tq.UnblockAll()
		t.Fatal("NextTask did not return after its context was canceled (parked in cond.Wait with no signal)")
	}
}

// TestQueueNextTaskCanClaimFilter guards NextTask's name-filtering: every other
// test calls it with a nil canClaim, so the "skip tasks this worker cannot
// claim, take the right one, leave the rest Waiting" path has no direct
// coverage at the queue level.
func TestQueueNextTaskCanClaimFilter(t *testing.T) {
	ctx := context.Background()
	tq := newTestQueue(10)
	// Insertion order fixes the order NextTask scans in.
	idA, _ := tq.Add("a", nil)
	_, _ = tq.Add("b", nil)
	idC, _ := tq.Add("c", nil)

	// A filter that admits only "c": NextTask must skip a and b, claim c, and
	// leave a and b Waiting.
	gotID, gotName, _, err := tq.NextTask(ctx, func(name string) bool { return name == "c" })
	if err != nil {
		t.Fatalf("NextTask: %v", err)
	}
	if gotName != "c" || gotID != idC {
		t.Fatalf("expected to claim c (%v), got %q (%v)", idC, gotName, gotID)
	}
	if info, _ := tq.Get(ctx, idA); info.Status != TaskStatusWaiting {
		t.Errorf("expected a to stay Waiting after a filtered claim, got %s", info.Status.Str())
	}

	// A filter that admits a and b now claims a — first in scan order.
	gotID2, gotName2, _, err := tq.NextTask(ctx, func(name string) bool { return name == "a" || name == "b" })
	if err != nil {
		t.Fatalf("NextTask: %v", err)
	}
	if gotName2 != "a" || gotID2 != idA {
		t.Fatalf("expected to claim a (%v) first, got %q (%v)", idA, gotName2, gotID2)
	}
}

func TestQueueConcurrency(t *testing.T) {
	ctx := context.Background()
	t.Run("concurrent operations", func(t *testing.T) {
		tq := newTestQueue(200)
		numTasks := 10
		for i := 0; i < numTasks; i++ {
			_, _ = tq.Add(myActionName, nil)
		}

		var wg sync.WaitGroup
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = tq.Add(myActionName, nil)
			}()
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = tq.List(ctx)
			}()
		}
		wg.Wait()

		list, _ := tq.List(ctx)
		if len(list) < 10 {
			t.Errorf("expected at least 10 tasks, got %d", len(list))
		}
	})

	t.Run("concurrent set status", func(t *testing.T) {
		tq := newTestQueue(100)
		ids := make([]uuid.UUID, 10)
		for i := range ids {
			ids[i], _ = tq.Add(myActionName, nil)
		}

		var wg sync.WaitGroup
		for i, id := range ids {
			wg.Add(1)
			go func(taskID uuid.UUID, idx int) {
				defer wg.Done()
				if idx%2 == 0 {
					_ = tq.SetStatus(ctx, taskID, TaskStatusRunning, time.Time{}, time.Time{})
				} else {
					_ = tq.SetStatus(ctx, taskID, TaskStatusComplete, time.Time{}, time.Now())
				}
			}(id, i)
		}
		wg.Wait()

		expectedByID := make(map[uuid.UUID]TaskStatus)
		for i, id := range ids {
			if i%2 == 0 {
				expectedByID[id] = TaskStatusRunning
			} else {
				expectedByID[id] = TaskStatusComplete
			}
		}
		list, _ := tq.List(ctx)
		for _, task := range list {
			expected := expectedByID[task.ID]
			if task.Status != expected {
				t.Errorf("task %v: expected %v, got %v", task.ID, expected, task.Status)
			}
		}
	})
}

func TestQueueEdgeCases(t *testing.T) {
	ctx := context.Background()
	t.Run("operations consistency", func(t *testing.T) {
		tq := newTestQueue(10)
		numTasks := 5
		for i := 0; i < numTasks; i++ {
			_, _ = tq.Add(myActionName, nil)
		}

		list, _ := tq.List(ctx)
		count := countStatus(list, TaskStatusWaiting)

		if len(list) != numTasks {
			t.Errorf("List: expected %d, got %d", numTasks, len(list))
		}
		if count != numTasks {
			t.Errorf("CountStatus(Waiting): expected %d, got %d", numTasks, count)
		}
	})

	t.Run("capacity limits", func(t *testing.T) {
		for _, capacity := range []int{1, 5, 15} {
			t.Run(fmt.Sprintf("capacity_%d", capacity), func(t *testing.T) {
				tq := newTestQueue(capacity)
				for i := 0; i < capacity; i++ {
					if _, err := tq.Add(myActionName, nil); err != nil {
						t.Fatalf("error at task %d: %v", i, err)
					}
				}
				if _, err := tq.Add(myActionName, nil); !errors.Is(err, ErrQueueFull) {
					t.Errorf("expected ErrQueueFull, got: %v", err)
				}
			})
		}
	})
}

func TestQueueEE2E(t *testing.T) {
	ctx := context.Background()
	t.Run("complete workflow", func(t *testing.T) {
		tq := newTestQueue(20)
		addedIDs := make([]uuid.UUID, 10)
		for i := range addedIDs {
			addedIDs[i], _ = tq.Add(myActionName, nil)
		}

		list, _ := tq.List(ctx)
		waiting := countStatus(list, TaskStatusWaiting)
		if waiting != 10 {
			t.Errorf("expected 10 waiting tasks, got %d", waiting)
		}
		if len(list) != 10 {
			t.Errorf("expected 10 tasks in list, got %d", len(list))
		}

		taskIDs := make(map[uuid.UUID]bool)
		for _, task := range list {
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
			if _, err := tq.Add(myActionName, nil); err != nil {
				t.Fatalf("error at task %d: %v", i, err)
			}
		}

		list, _ := tq.List(ctx)
		if len(list) != capacity {
			t.Errorf("unexpected count: got %d want %d", len(list), capacity)
		}
		if _, err := tq.Add(myActionName, nil); err == nil {
			t.Error("expected error when adding to full TaskQueue")
		}
	})
}

func TestQueueParamsRoundTrip(t *testing.T) {
	ctx := context.Background()
	tq := newTestQueue(10)
	payload := []byte(`{"mode":"full"}`)

	id, err := tq.Add("scan", payload)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	info, err := tq.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(info.Params) != string(payload) {
		t.Errorf("Get params: got %q want %q", info.Params, payload)
	}

	gotID, name, params, err := tq.NextTask(ctx, nil)
	if err != nil {
		t.Fatalf("NextTask: %v", err)
	}
	if gotID != id || name != "scan" {
		t.Errorf("NextTask id/name: got %v/%q", gotID, name)
	}
	if string(params) != string(payload) {
		t.Errorf("NextTask params: got %q want %q", params, payload)
	}
}

type recoverableMemPersistence struct {
	mu    sync.Mutex
	tasks map[uuid.UUID]TaskInfo
}

func newRecoverableMemPersistence() *recoverableMemPersistence {
	return &recoverableMemPersistence{tasks: make(map[uuid.UUID]TaskInfo)}
}

func (m *recoverableMemPersistence) SaveTask(ctx context.Context, t TaskInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tasks[t.ID] = t
	return nil
}

func (m *recoverableMemPersistence) RemoveTasks(ctx context.Context, ids []uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range ids {
		delete(m.tasks, id)
	}
	return nil
}

func (m *recoverableMemPersistence) List(ctx context.Context) ([]TaskInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]TaskInfo, 0, len(m.tasks))
	for _, t := range m.tasks {
		out = append(out, t)
	}
	return out, nil
}

// TestQueueRecoveryMarksInFlightRunningAsFailed pins the intended recovery
// behavior: a task persisted as Running (in-flight when the process died) must
// come back terminal, not stuck Running. NextTask only ever claims Waiting
// tasks and CleanHistory only trims terminal ones, so a recovered Running task
// is otherwise orphaned forever — never re-run, never cleaned.
//
// EXPECTED TO FAIL until NewTaskQueue rewrites a recovered Running status to
// Failed on load.
func TestQueueRecoveryMarksInFlightRunningAsFailed(t *testing.T) {
	ctx := context.Background()
	persist := newRecoverableMemPersistence()
	id := uuid.New()
	// Simulate a crash: the task was Running when the process died.
	if err := persist.SaveTask(ctx, TaskInfo{
		ID:        id,
		Name:      "scan",
		Status:    TaskStatusRunning,
		QueuedAt:  time.Now(),
		StartedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	tq := NewTaskQueue(TaskQueueCfg{QueueSize: 10, HistorySize: 10, Persistence: persist})
	info, err := tq.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get after recovery: %v", err)
	}
	if info.Status != TaskStatusFailed {
		t.Errorf("recovered in-flight task: got status %s, want failed (it is otherwise orphaned)", info.Status.Str())
	}
}

func TestQueueParamsRecovered(t *testing.T) {
	ctx := context.Background()
	persist := newRecoverableMemPersistence()

	tq1 := NewTaskQueue(TaskQueueCfg{QueueSize: 10, HistorySize: 10, Persistence: persist})
	id, err := tq1.Add("scan", []byte(`{"mode":"full"}`))
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	// New queue from the same persistence simulates a restart.
	tq2 := NewTaskQueue(TaskQueueCfg{QueueSize: 10, HistorySize: 10, Persistence: persist})
	info, err := tq2.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get after recovery: %v", err)
	}
	if string(info.Params) != `{"mode":"full"}` {
		t.Errorf("recovered params: got %q", info.Params)
	}
}
