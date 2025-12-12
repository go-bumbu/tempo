package tempo

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"sync"
	"time"
)

// TaskQueue Allows to manage the lifecycle of tasks in a queue: add tasks to the queue,
// change the status, get the status etc. It is not responsible for running the tasks or to handle the task.
// it is generally threadsafe except explicit unsafe methods.
type TaskQueue struct {
	mu    sync.Mutex
	cond  *sync.Cond
	tasks []*QueuedTask // ideally to avoid iterators this should be a sorted map

	maxWaiting int
	maxDone    int
}

type QueueCfg struct {
	QueueSize   int
	HistorySize int // todo clean history data
}

func NewTaskQueue(cfg QueueCfg) *TaskQueue {
	t := TaskQueue{
		mu:    sync.Mutex{},
		tasks: []*QueuedTask{},

		maxWaiting: cfg.QueueSize,
		maxDone:    cfg.HistorySize,
	}
	t.cond = sync.NewCond(&t.mu)
	return &t
}

var ErrQueueFull = errors.New("TaskQueue full")

type TaskStatus int

const (
	TaskStatusWaiting TaskStatus = iota
	TaskStatusRunning
	TaskStatusComplete
	TaskStatusFailed
	TaskStatusPanicked
	TaskStatusCanceled
)

func (s TaskStatus) Str() string {
	switch s {
	case TaskStatusWaiting:
		return "waiting"
	case TaskStatusRunning:
		return "running"
	case TaskStatusComplete:
		return "complete"
	case TaskStatusFailed:
		return "failed"
	case TaskStatusPanicked:
		return "panicked"
	case TaskStatusCanceled:
		return "canceled"
	default:
		return "unknown"
	}
}

type QueuedTask struct {
	id     uuid.UUID
	name   string
	Task   func(ctx context.Context) error
	Status TaskStatus
	err    error

	QueuedAt  time.Time
	StartedAt time.Time
	EndedAt   time.Time

	// handle the individual task
	done     chan struct{}
	cancelFn context.CancelFunc
}

func (q *TaskQueue) Add(fn func(ctx context.Context) error, name string) (uuid.UUID, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.countStatusUnsafe(TaskStatusWaiting) >= q.maxWaiting {
		return uuid.Nil, ErrQueueFull
	}

	id := uuid.New()
	q.tasks = append(q.tasks, &QueuedTask{
		id:        id,
		name:      name,
		Task:      fn,
		Status:    TaskStatusWaiting,
		QueuedAt:  time.Now(),
		StartedAt: time.Time{},
		done:      make(chan struct{}),
	})

	q.cond.Signal()
	return id, nil
}

type TaskInfo struct {
	ID        uuid.UUID
	Name      string
	Status    TaskStatus
	QueuedAt  time.Time
	StartedAt time.Time
	EndedAt   time.Time
}

func (q *TaskQueue) List() []TaskInfo {
	q.mu.Lock()
	defer q.mu.Unlock()

	var info []TaskInfo
	for _, task := range q.tasks {
		qt := TaskInfo{
			ID:        task.id,
			Name:      task.name,
			Status:    task.Status,
			QueuedAt:  task.QueuedAt,
			StartedAt: task.StartedAt,
			EndedAt:   task.EndedAt,
		}
		info = append(info, qt)
	}
	return info
}

// todo add test
func (q *TaskQueue) getTask(id uuid.UUID) (*QueuedTask, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for i := range q.tasks {
		if q.tasks[i].id == id {
			return q.tasks[i], nil
		}
	}
	return nil, ErrTaskNotFound
}

func (q *TaskQueue) SetStatus(id uuid.UUID, status TaskStatus, err error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.setStatusUnsafe(id, status, err)
}

func (q *TaskQueue) setStatusUnsafe(id uuid.UUID, status TaskStatus, err error) {
	for i := range q.tasks {
		if q.tasks[i].id == id {
			q.tasks[i].Status = status
			if err != nil {
				q.tasks[i].err = err
			}
			return
		}
	}
}

func (q *TaskQueue) CountStatus(status TaskStatus) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.countStatusUnsafe(status)
}

func (q *TaskQueue) countStatusUnsafe(status TaskStatus) int {
	n := 0
	for i := range q.tasks {
		if q.tasks[i].Status == status {
			n++
		}
	}
	return n
}

var ErrTaskNotFound = errors.New("task not found")

// WaitAndClaimTask blocks the execution until either the context is canceled or
// a new task can be executed, in the later case a pointer to the task is returned
func (q *TaskQueue) WaitAndClaimTask(ctx context.Context) (*QueuedTask, error) {
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

func (q *TaskQueue) CleanHistory() {
	q.mu.Lock()
	defer q.mu.Unlock()
	// todo
	// itterate over all tasks, count the ones in terminal status
	// itterate a second time and remove onlu leaving N
}

func (q *TaskQueue) UnlockAllWaiting() {
	q.cond.Broadcast()
}
