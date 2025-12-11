package tempo

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"sync"
	"time"
)

type Queue struct {
	mu    sync.Mutex
	cond  *sync.Cond
	tasks []*QueuedTask // ideally to avoid iterators this should be a sorted map

	maxWaiting int
	maxRunning int
	maxDone    int
}

type QueueCfg struct {
	QueueSize      int
	MaxParallelism int
	HistorySize    int
}

func NewQueue(cfg QueueCfg) *Queue {
	t := Queue{
		mu:    sync.Mutex{},
		tasks: []*QueuedTask{},

		maxWaiting: cfg.QueueSize,
		maxRunning: cfg.MaxParallelism,
		maxDone:    cfg.HistorySize,
	}
	t.cond = sync.NewCond(&t.mu)
	return &t
}

var ErrQueueFull = errors.New("queue full")

type TaskStatus int

const (
	TaskStatusWaiting TaskStatus = iota
	TaskStatusRunning
	TaskStatusComplete
)

func (s TaskStatus) Str() string {
	switch s {
	case TaskStatusWaiting:
		return "waiting"
	case TaskStatusRunning:
		return "running"
	case TaskStatusComplete:
		return "complete"
	default:
		return "unknown"
	}
}

type QueuedTask struct {
	ID        uuid.UUID                 // Unique identifier for the task
	Task      func(ctx context.Context) // The actual task (for simple functions)
	Status    TaskStatus
	QueuedAt  time.Time // When it was added to queue
	StartedAt time.Time // When it started running
}

func (q *Queue) Add(fn func(ctx context.Context)) (uuid.UUID, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.countUnsafe(TaskStatusWaiting) >= q.maxWaiting {
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

type QueueTaskInfo struct {
	ID        uuid.UUID
	Status    TaskStatus
	QueuedAt  time.Time
	StartedAt time.Time
}

func (q *Queue) List() []QueueTaskInfo {
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

func (q *Queue) HasWaiting() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.hasWaitingUnsafe()
}

func (q *Queue) hasWaitingUnsafe() bool {
	for i, _ := range q.tasks {
		if q.tasks[i].Status == TaskStatusWaiting {
			return true
		}
	}
	return false
}

func (q *Queue) SetStatus(id uuid.UUID, status TaskStatus) {
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

func (q *Queue) CountStatus(status TaskStatus) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.countUnsafe(status)
}

func (q *Queue) countUnsafe(status TaskStatus) int {
	n := 0
	for i, _ := range q.tasks {
		if q.tasks[i].Status == status {
			n++
		}
	}
	return n
}

var ErrTaskNotFound = errors.New("task not found")

func (q *Queue) WaitAndClaimTask(ctx context.Context) (*QueuedTask, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for {
		// Try to claim a waiting task
		for i := range q.tasks {
			if q.tasks[i].Status == TaskStatusWaiting {
				q.tasks[i].Status = TaskStatusRunning
				q.tasks[i].StartedAt = time.Now()
				return q.tasks[i], nil
			}
		}

		// No tasks available - wait for signal or cancellation
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			q.cond.Wait()
		}
	}
}

func (q *Queue) WaitForTask(ctx context.Context) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	for !q.hasWaitingUnsafe() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			q.cond.Wait()
		}
	}
	return nil
}

func (q *Queue) StartTask() (*QueuedTask, error) {
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

func (q *Queue) Unlock() {
	q.cond.Signal()
}

func (q *Queue) UnlockAll() {
	q.cond.Broadcast()
}

func (q *Queue) Cancel() {
	panic("not implemented")
}
