package tempo

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"sync"
	"time"
)

type TaskQueue struct {
	mu    sync.Mutex
	cond  *sync.Cond
	tasks []*QueuedTask // ideally to avoid iterators this should be a sorted map

	maxWaiting int
	maxRunning int
	maxDone    int
}

type TaskQueueCfg struct {
	QueueSize      int
	MaxParallelism int
	HistorySize    int
}

func NewTaskQueue(cfg TaskQueueCfg) *TaskQueue {
	t := TaskQueue{
		mu:    sync.Mutex{},
		tasks: []*QueuedTask{},

		maxWaiting: cfg.QueueSize,
		maxRunning: cfg.MaxParallelism,
		maxDone:    cfg.HistorySize,
	}
	t.cond = sync.NewCond(&t.mu)
	return &t
}

func (q *TaskQueue) Add(fn func(ctx context.Context)) (uuid.UUID, error) {

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.CountStatus(TaskStatusWaiting) >= q.maxWaiting {
		return uuid.Nil, ErrQueueFull
	}

	id := uuid.New()
	q.tasks = append(q.tasks, &QueuedTask{
		ID:        id,
		Task:      fn,
		Status:    TaskStatusWaiting,
		QueuedAt:  time.Now(),
		StartedAt: time.Time{},
	})

	q.cond.Signal()
	return id, nil
}

func (q *TaskQueue) List() []QueueTaskInfo {
	q.mu.Lock()
	defer q.mu.Unlock()

	var info []QueueTaskInfo
	for _, task := range q.tasks {
		qt := QueueTaskInfo{
			ID:        task.ID,
			Status:    task.Status,
			QueuedAt:  task.QueuedAt,
			StartedAt: task.StartedAt,
		}
		info = append(info, qt)
	}
	return info
}

func (q *TaskQueue) HasWaiting() bool {
	for i, _ := range q.tasks {
		if q.tasks[i].Status == TaskStatusWaiting {
			return true
		}
	}
	return false
}

func (q *TaskQueue) SetStatus(id uuid.UUID, status TaskStatus) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, _ := range q.tasks {
		if q.tasks[i].ID == id {
			q.tasks[i].Status = status
			return
		}
	}
	return
}

func (q *TaskQueue) CountStatus(status TaskStatus) int {
	n := 0
	for i, _ := range q.tasks {
		if q.tasks[i].Status == status {
			n++
		}
	}
	return n
}

var ErrTaskNotFound = errors.New("task not found")

func (q *TaskQueue) WaitForTask(ctx context.Context) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	for !q.HasWaiting() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			q.cond.Wait()
		}
	}
	return nil
}

func (q *TaskQueue) StartWaiting() (*QueuedTask, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, _ := range q.tasks {
		if q.tasks[i].Status == TaskStatusWaiting {
			q.tasks[i].Status = TaskStatusRunning
			return q.tasks[i], nil
		}
	}
	return nil, ErrTaskNotFound
}

func (q *TaskQueue) GetFirst(status TaskStatus) (*QueuedTask, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, _ := range q.tasks {
		if q.tasks[i].Status == status {
			return q.tasks[i], nil
		}
	}
	return nil, ErrTaskNotFound
}

func (q *TaskQueue) Unlock() {
	q.cond.Signal()
}

func (q *TaskQueue) Cancel() {
	panic("not implemented")
}
