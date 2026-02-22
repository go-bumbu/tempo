package tempo

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
)

var ErrQueueFull = errors.New("TaskQueue full")

type TaskStatus int

const (
	TaskStatusWaiting TaskStatus = iota
	TaskStatusRunning
	TaskStatusComplete
	TaskStatusFailed
	TaskStatusPanicked
	TaskStatusCanceled
	TaskStatusCancelError
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
	case TaskStatusCancelError:
		return "cancel_error"
	default:
		return "unknown"
	}
}

// TaskInfo holds task metadata (id, name, status, times).
type TaskInfo struct {
	ID        uuid.UUID
	Name      string
	Status    TaskStatus
	QueuedAt  time.Time
	StartedAt time.Time
	EndedAt   time.Time
}

var ErrTaskNotFound = errors.New("task not found")

var TaskTerminalStatus = []TaskStatus{
	TaskStatusComplete,
	TaskStatusFailed,
	TaskStatusPanicked,
	TaskStatusCanceled,
}

// TaskStatePersistence persists task state. The queue calls it only to mirror
// state changes; no blocking, no concurrency contract, no shutdown.
type TaskStatePersistence interface {
	// SaveTask is called when a task is added or when its status/times change. Upsert by id.
	SaveTask(ctx context.Context, task TaskInfo) error
	// RemoveTasks is called when the queue trims terminal tasks in CleanHistory.
	RemoveTasks(ctx context.Context, ids []uuid.UUID) error
}

// RecoverablePersistence can load persisted tasks for recovery (e.g. after restart).
type RecoverablePersistence interface {
	TaskStatePersistence
	List(ctx context.Context) ([]TaskInfo, error)
}

// MemPersistence is an in-memory implementation of TaskStatePersistence.
// It is used by default when no external persistence is provided.
type MemPersistence struct {
	mu    sync.Mutex
	tasks map[uuid.UUID]TaskInfo
}

// NewMemPersistence returns a new in-memory persistence.
func NewMemPersistence() *MemPersistence {
	return &MemPersistence{tasks: make(map[uuid.UUID]TaskInfo)}
}

func (m *MemPersistence) SaveTask(ctx context.Context, task TaskInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tasks[task.ID] = task
	return nil
}

func (m *MemPersistence) RemoveTasks(ctx context.Context, ids []uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range ids {
		delete(m.tasks, id)
	}
	return nil
}

// taskRecord holds metadata for one task in the queue.
type taskRecord struct {
	id        uuid.UUID
	name      string
	status    TaskStatus
	queuedAt  time.Time
	startedAt time.Time
	endedAt   time.Time
}

// TaskQueue holds the main logic for managing the task queue: concurrent atomic
// operations, blocking for next task, and shutdown. Optional TaskStatePersistence
// is used only to mirror state changes; it has no concurrency or shutdown concerns.
type TaskQueue struct {
	mu         sync.Mutex
	cond       *sync.Cond
	tasks      []*taskRecord
	maxWaiting int
	maxDone    int
	persist    TaskStatePersistence
}

// TaskQueueCfg configures queue capacity, history size, and persistence.
type TaskQueueCfg struct {
	QueueSize   int
	HistorySize int
	// Persistence mirrors task state; if nil, in-memory persistence is used.
	Persistence TaskStatePersistence
}

// NewTaskQueue creates a TaskQueue from cfg. State is mirrored to cfg.Persistence; if nil, in-memory persistence is used.
func NewTaskQueue(cfg TaskQueueCfg) *TaskQueue {
	if cfg.HistorySize <= 0 {
		cfg.HistorySize = 10
	}
	p := cfg.Persistence
	if p == nil {
		p = NewMemPersistence()
	}
	q := &TaskQueue{
		tasks:      make([]*taskRecord, 0),
		maxWaiting: cfg.QueueSize,
		maxDone:    cfg.HistorySize,
		persist:    p,
	}
	q.cond = sync.NewCond(&q.mu)
	if r, ok := p.(RecoverablePersistence); ok {
		if list, err := r.List(context.Background()); err == nil && len(list) > 0 {
			for _, info := range list {
				q.tasks = append(q.tasks, &taskRecord{
					id:        info.ID,
					name:      info.Name,
					status:    info.Status,
					queuedAt:  info.QueuedAt,
					startedAt: info.StartedAt,
					endedAt:   info.EndedAt,
				})
			}
		}
	}
	return q
}

// Add enqueues a task by name. Returns the new task id or ErrQueueFull.
func (q *TaskQueue) Add(name string) (uuid.UUID, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.countStatusUnsafe(TaskStatusWaiting) >= q.maxWaiting {
		return uuid.Nil, ErrQueueFull
	}
	id := uuid.New()
	now := time.Now()
	t := &taskRecord{
		id:       id,
		name:     name,
		status:   TaskStatusWaiting,
		queuedAt: now,
	}
	q.tasks = append(q.tasks, t)
	_ = q.persist.SaveTask(context.Background(), q.recordToInfo(t))
	q.cond.Signal()
	return id, nil
}

// NextTask returns the next task to run, atomically marking it Running.
// Blocks until an eligible task exists or ctx is done. canClaim filters by task name.
func (q *TaskQueue) NextTask(ctx context.Context, canClaim func(name string) bool) (uuid.UUID, string, error) {
	q.mu.Lock()
	for {
		for _, t := range q.tasks {
			if t.status != TaskStatusWaiting {
				continue
			}
			if canClaim != nil && !canClaim(t.name) {
				continue
			}
			t.status = TaskStatusRunning
			t.startedAt = time.Now()
			_ = q.persist.SaveTask(context.Background(), q.recordToInfo(t))
			id, name := t.id, t.name
			q.mu.Unlock()
			return id, name, nil
		}
		select {
		case <-ctx.Done():
			q.mu.Unlock()
			return uuid.Nil, "", ctx.Err()
		default:
			q.cond.Wait()
		}
	}
}

// List returns all tasks, ordered by QueuedAt newest first.
func (q *TaskQueue) List(ctx context.Context) ([]TaskInfo, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	info := make([]TaskInfo, 0, len(q.tasks))
	for _, t := range q.tasks {
		info = append(info, q.recordToInfo(t))
	}
	slices.SortFunc(info, func(a, b TaskInfo) int {
		return b.QueuedAt.Compare(a.QueuedAt)
	})
	return info, nil
}

// Get returns task metadata by id.
func (q *TaskQueue) Get(ctx context.Context, id uuid.UUID) (TaskInfo, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, t := range q.tasks {
		if t.id == id {
			return q.recordToInfo(t), nil
		}
	}
	return TaskInfo{}, ErrTaskNotFound
}

// SetStatus updates the task's status and optionally started/ended times.
func (q *TaskQueue) SetStatus(ctx context.Context, id uuid.UUID, status TaskStatus, startedAt, endedAt time.Time) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, t := range q.tasks {
		if t.id == id {
			t.status = status
			if !startedAt.IsZero() {
				t.startedAt = startedAt
			}
			if !endedAt.IsZero() {
				t.endedAt = endedAt
			}
			_ = q.persist.SaveTask(ctx, q.recordToInfo(t))
			return nil
		}
	}
	return ErrTaskNotFound
}

// CleanHistory removes old terminal tasks so that at most maxDone remain.
func (q *TaskQueue) CleanHistory(ctx context.Context, maxDone int) error {
	if maxDone <= 0 {
		maxDone = q.maxDone
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	doneCount := 0
	for _, t := range q.tasks {
		if slices.Contains(TaskTerminalStatus, t.status) {
			doneCount++
		}
	}
	if maxDone >= doneCount {
		return nil
	}
	skip := doneCount - maxDone
	var toRemove []uuid.UUID
	filtered := q.tasks[:0]
	for _, t := range q.tasks {
		if slices.Contains(TaskTerminalStatus, t.status) && skip > 0 {
			toRemove = append(toRemove, t.id)
			skip--
			continue
		}
		filtered = append(filtered, t)
	}
	q.tasks = filtered
	_ = q.persist.RemoveTasks(ctx, toRemove)
	return nil
}

// UnblockAll wakes goroutines blocked in NextTask (e.g. for shutdown).
func (q *TaskQueue) UnblockAll() {
	q.cond.Broadcast()
}

func (q *TaskQueue) recordToInfo(t *taskRecord) TaskInfo {
	return TaskInfo{
		ID:        t.id,
		Name:      t.name,
		Status:    t.status,
		QueuedAt:  t.queuedAt,
		StartedAt: t.startedAt,
		EndedAt:   t.endedAt,
	}
}

func (q *TaskQueue) countStatusUnsafe(status TaskStatus) int {
	n := 0
	for _, t := range q.tasks {
		if t.status == status {
			n++
		}
	}
	return n
}
