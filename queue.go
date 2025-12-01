package tempo

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"sync"
	"time"
)

type Queue struct {
	maxParallelism int           // Max concurrent tasks for this queue
	queueSize      int           // Max total tasks (waiting + running)
	tasks          []*QueuedTask // All tasks (waiting + running)
	lock           sync.Mutex

	// runtime context

	workerGroup    sync.WaitGroup
	scheduleNotify chan struct{} // Notification channel for scheduler wake-up

	// handle shutdown
	ctx      context.Context
	cancel   context.CancelFunc
	stopOnce sync.Once
	stopChan chan struct{}
}

// QueuedTask wraps a task with metadata
type QueuedTask struct {
	ID        uuid.UUID                 // Unique identifier for the task
	Task      func(ctx context.Context) // The actual task (for simple functions)
	Status    TaskStatus                // Current status
	QueuedAt  time.Time                 // When it was added to queue
	StartedAt time.Time                 // When it started running
}

// TaskStatus represents the current state of a task
type TaskStatus string

const (
	TaskStatusWaiting  TaskStatus = "waiting"
	TaskStatusRunning  TaskStatus = "running"
	TaskStatusComplete TaskStatus = "completed"
)

func NewQueue(parallel, size int) *Queue {

	ctx, cancel := context.WithCancel(context.Background())

	return &Queue{
		maxParallelism: parallel,
		queueSize:      size,
		tasks:          []*QueuedTask{},
		lock:           sync.Mutex{},

		scheduleNotify: make(chan struct{}),

		// shutdown
		ctx:      ctx,
		cancel:   cancel,
		stopOnce: sync.Once{},
		stopChan: make(chan struct{}),
	}
}

func (q *Queue) Start() {
	go q.taskScheduler()
}

func (q *Queue) taskScheduler() {
	for {
		select {
		case <-q.ctx.Done(): // do nothing if the queue is stopped
			return
		case <-q.scheduleNotify:
			// todo also notify the scheduler once a task is done
			q.tryRun()
		}
	}
}

func (q *Queue) tryRun() {

	q.lock.Lock()
	defer q.lock.Unlock()

	runningCount := q.countByStatusUnsafe(TaskStatusRunning)
	if runningCount >= q.maxParallelism {
		return
	}

	// get the first task in the list
	var task *QueuedTask

	for _, item := range q.tasks {
		if item.Status == TaskStatusWaiting {
			task = item
			break
		}
	}
	// if no waiting task found, just return
	if task == nil {
		return
	}

	task.Status = TaskStatusRunning
	task.StartedAt = time.Now()

	q.workerGroup.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				// todo => handle recover
				//spew.Dump("recover")
				//fmt.Println(r)
			}
			q.workerGroup.Done()
		}()

		task.Task(q.ctx)
		// Todo set completion message and status depending on function
		// panic for unrecoverable errors
		// error for warnings, the caller might decide to continue or stop on error
		q.completeTask(task)
	}()

}

func (q *Queue) completeTask(task *QueuedTask) {
	q.lock.Lock()
	task.Status = TaskStatusComplete
	// todo, cleanup older completed jobs
	q.lock.Unlock()

	// notify the scheduler that a task was completed
	if q.ctx.Err() == nil {
		select {
		case q.scheduleNotify <- struct{}{}:
		default:
		}
	}
}

// countByStatusUnsafe iterates over the tasks and counts the amount by status
// note that this is not thread safe, and the calling function needs to lock the execution
func (q *Queue) countByStatusUnsafe(status TaskStatus) int {
	c := 0
	for _, task := range q.tasks {
		if task.Status == status {
			c++
		}
	}
	return c
}

func (q *Queue) Wait() {
	<-q.stopChan
}

var ErrUnsafeStop = errors.New("unsafe stop: some workers failed to shutdown")

func (q *Queue) ShutDown(ctx context.Context) error {

	var err error

	q.stopOnce.Do(func() {
		q.cancel()              // notify running jobs to stop
		close(q.scheduleNotify) // Close notification channel to stop scheduler

		shutdownCh := make(chan struct{})
		go func() {
			q.workerGroup.Wait()
			close(shutdownCh)
		}()

		select {
		case <-shutdownCh:
			err = nil
		case <-ctx.Done():
			err = ErrUnsafeStop
		}

		// Unblock Wait() if it's waiting
		close(q.stopChan)
	})
	return err
}

func (q *Queue) Add(task func(ctx context.Context)) (uuid.UUID, error) {
	q.lock.Lock()
	waiting := q.countByStatusUnsafe(TaskStatusWaiting)

	if waiting >= q.queueSize {
		q.lock.Unlock()
		return uuid.UUID{}, ErrQueueFull
	}

	taskId := uuid.New()
	qt := &QueuedTask{
		ID:       taskId,
		Task:     task,
		Status:   TaskStatusWaiting,
		QueuedAt: time.Now(),
	}

	q.tasks = append(q.tasks, qt)
	q.lock.Unlock()

	// notify the scheduler that a task was added
	q.scheduleNotify <- struct{}{}
	return taskId, nil
}

type QueueTaskInfo struct {
	ID        uuid.UUID
	Status    TaskStatus
	QueuedAt  time.Time
	StartedAt time.Time
}
type QueueInfo struct {
	PoolSize  int
	QueueSize int
	Tasks     []QueueTaskInfo // every task should have status like running and queued datetime
}

func (q *Queue) List() QueueInfo {
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
	return info
}

func (q *Queue) CancelJob() {
	//todo cancel running job
	// todo cancel pending job

}
