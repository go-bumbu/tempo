package tempo

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"sync"
	"time"
)

// TaskStatus represents the current state of a task
type TaskStatus string

const (
	TaskStatusWaiting TaskStatus = "waiting"
	TaskStatusRunning TaskStatus = "running"
)

// QueuedTask wraps a task with metadata
type QueuedTask struct {
	ID        string     // Unique identifier for the task
	Task      func()     // The actual task
	Status    TaskStatus // Current status
	QueuedAt  time.Time  // When it was added to queue
	StartedAt *time.Time // When it started running (nil if not started)

}

type queue struct {
	name           string
	maxParallelism int           // Max concurrent tasks for this queue
	queueSize      int           // Max total tasks (waiting + running)
	tasks          []*QueuedTask // All tasks (waiting + running)
	lock           sync.Mutex
}

// countByStatus counts tasks with a specific status (must be called with lock held)
func (q *queue) countByStatus(status TaskStatus) int {
	count := 0
	for _, qt := range q.tasks {
		if qt.Status == status {
			count++
		}
	}
	return count
}

type queueHandler struct {
	taskQueues map[string]*queue
}

func (h *queueHandler) getQueue(name string) (*queue, error) {
	got, ok := h.taskQueues[name]
	if !ok {
		return nil, fmt.Errorf("queue %s not found", name)
	}
	return got, nil
}

// RegisterQueue adds a new queue with its own parallelism settings
func (h *queueHandler) RegisterQueue(name string, maxParallelism, queueSize int) {
	h.taskQueues[name] = &queue{
		name:           name,
		maxParallelism: maxParallelism,
		queueSize:      queueSize,
		tasks:          make([]*QueuedTask, 0, queueSize),
	}
}

// AddTask adds a task to the waiting queue
func (h *queueHandler) AddTask(queueName string, task func()) (string, error) {

	q, err := h.getQueue(queueName)
	if err != nil {
		return "", err
	}

	q.lock.Lock()
	defer q.lock.Unlock()

	// Check total capacity (waiting + running)
	if len(q.tasks) >= q.queueSize {
		return "", ErrQueueFull
	}

	taskId := uuid.New().String()
	qt := &QueuedTask{
		ID:       taskId,
		Task:     task,
		Status:   TaskStatusWaiting,
		QueuedAt: time.Now(),
	}

	q.tasks = append(q.tasks, qt)
	return taskId, nil
}

type QueueInfo struct {
	PoolSize  int
	QueueSize int
	Tasks     []QueueTaskInfo // every task should have status like running and queued datetime
}

type QueueTaskInfo struct {
	ID        string
	Status    TaskStatus
	QueuedAt  time.Time
	StartedAt *time.Time
}

func (h *queueHandler) ListTask(queueName string) (QueueInfo, error) {
	q, err := h.getQueue(queueName)
	if err != nil {
		return QueueInfo{}, err
	}

	q.lock.Lock()
	defer q.lock.Unlock()

	info := QueueInfo{
		PoolSize:  q.maxParallelism,
		QueueSize: q.queueSize,
	}

	for _, task := range q.tasks {
		qt := QueueTaskInfo{
			ID:        task.ID,
			Status:    task.Status,
			QueuedAt:  task.QueuedAt,
			StartedAt: task.StartedAt,
		}
		info.Tasks = append(info.Tasks, qt)
	}
	return info, nil
}

var ErrTaskNotFound = errors.New("task not found")

//// CanRun checks if we can start another task from this queue
//func (h *queueHandler) CanRun(queueName string) (bool, error) {
//
//	q, err := h.getQueue(queueName)
//	if err != nil {
//		return false, err
//	}
//
//	q.lock.Lock()
//	defer q.lock.Unlock()
//
//	runningCount := q.countByStatus(TaskStatusRunning)
//	waitingCount := q.countByStatus(TaskStatusWaiting)
//
//	return runningCount < q.maxParallelism && waitingCount > 0, nil
//}

func (h *queueHandler) StarNextTask(queueName string) error {
	q, err := h.getQueue(queueName)
	if err != nil {
		return err
	}

	q.lock.Lock()
	defer q.lock.Unlock()

	runningCount := q.countByStatus(TaskStatusRunning)
	waitingCount := q.countByStatus(TaskStatusWaiting)

	if waitingCount <= 0 {
		return fmt.Errorf("no waiting tasks found for queue %s", queueName)
	}

	if runningCount >= q.maxParallelism {
		return ErrQueueFull
	}

	for i, qt := range q.tasks {
		if qt.Status == TaskStatusWaiting {
			q.tasks[i].Status = TaskStatusRunning

			// create a worker that runs the task
			// the worker calls complete
			return nil
		}
	}
	return ErrTaskNotFound
}

func (h *queueHandler) CompleteTask(queueName, taskID string) error {
	q, err := h.getQueue(queueName)
	if err != nil {
		return err
	}

	q.lock.Lock()
	defer q.lock.Unlock()

	for i, qt := range q.tasks {
		if qt.ID == taskID {
			// Remove task from slice
			q.tasks = append(q.tasks[:i], q.tasks[i+1:]...)
			return nil
		}
	}
	return ErrTaskNotFound
}
