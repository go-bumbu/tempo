package tempo

import (
	"context"
	"errors"
	"github.com/davecgh/go-spew/spew"
	"github.com/google/uuid"
	"sync"
	"time"
)

type Queue struct {
	maxParallelism int // Max concurrent waitingTasks for this queue
	queueSize      int // Max total waitingTasks waiting
	historySize    int // amount of waitingTasks to keep in completed

	// runtime context
	tasks []*QueuedTask

	mu   sync.Mutex
	wg   sync.WaitGroup
	cond *sync.Cond

	// handle shutdown
	ctx      context.Context
	cancel   context.CancelFunc
	stopOnce sync.Once
	stopChan chan struct{}
}

// TaskStatus represents the current state of a task
type TaskStatus string

const (
	defaultMaxParallel = 1
	defaultQueueSize   = 0
	defaultHistory     = 10
)

type QueueCfg struct {
	QueueSize      int
	MaxParallelism int
	HistorySize    int
}

func NewQueue(cfg QueueCfg) *Queue {

	ctx, cancel := context.WithCancel(context.Background())

	q := &Queue{
		maxParallelism: defaultMaxParallel,
		queueSize:      defaultQueueSize,
		historySize:    defaultHistory,
		tasks:          []*QueuedTask{},
		mu:             sync.Mutex{},

		// shutdown
		ctx:      ctx,
		cancel:   cancel,
		stopOnce: sync.Once{},
		stopChan: make(chan struct{}),
	}
	q.cond = sync.NewCond(&q.mu)

	if cfg.MaxParallelism > 0 {
		q.maxParallelism = cfg.MaxParallelism
	}
	if cfg.QueueSize > 0 {
		q.queueSize = cfg.QueueSize
	}
	if cfg.HistorySize > 0 {
		q.historySize = cfg.HistorySize
	}

	return q
}

func (q *Queue) Start() {

	q.wg.Go(func() {
		<-q.ctx.Done()
		// Wake up waitForTask() for the last time during shutdown
		q.cond.Signal()
	})

	tasksCh := make(chan *QueuedTask)

	// main control loop
	q.wg.Go(func() {
		defer close(tasksCh)

		for {
			task, err := q.waitForTask(q.ctx)
			if err != nil {
				return
			}
			select {
			case tasksCh <- task:
				// Task sent, wait for the next one.
			case <-q.ctx.Done():
				return
			}
		}
	})

	// workers
	for range q.maxParallelism {
		q.wg.Go(func() {
			for task := range tasksCh {
				func() {
					defer func() {
						// If the task panics, record the panic as an error.
						if v := recover(); v != nil {
							spew.Dump(v)
							// todo handle error
							//task.data.err = fmt.Errorf("panic: %v", v)
						}
						// Drop the reference to the function, so it can be GC'ed.
						// task.data.f = nil
						// Close the task "done" channel, so Wait() unblocks.
						//close(task.data.done)
					}()

					q.mu.Lock()
					task.Status = TaskStatusRunning
					q.mu.Unlock()

					panic(task)

					// todo get error
					task.Task(q.ctx)
					//task.data.err = task.data.f(ctx)
				}()
			}
		})
	}

	// todo move to background, dont wait
	//q.wg.Wait()
	//
	//for _, t := range q.waitingTasks {
	//	close(t.data.done)
	//}
	//
	//q.waitingTasks = nil
}

func (q *Queue) waitForTask(ctx context.Context) (*QueuedTask, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for q.countTaskStatus(TaskStatusWaiting) == 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			q.cond.Wait()
		}
	}

	// get the first waiting task
	var task *QueuedTask
	for i, _ := range q.tasks {
		if q.tasks[i].Status == TaskStatusWaiting {
			task = q.tasks[i]
		}
	}

	if task == nil {
		panic("no task found")
	}

	return task, nil
}

func (q *Queue) countTaskStatus(in TaskStatus) int {
	n := 0
	for i, _ := range q.tasks {
		if q.tasks[i].Status == in {
			n++
		}
	}
	return n
}

func (q *Queue) Wait() {
	<-q.stopChan
}

var ErrUnsafeStop = errors.New("unsafe stop: some workers failed to shutdown")

func (q *Queue) ShutDown(ctx context.Context) error {

	var err error

	q.stopOnce.Do(func() {
		q.cancel() // notify running jobs to stop

		shutdownCh := make(chan struct{})
		go func() {
			q.wg.Wait()
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

var ErrQueueFull = errors.New("queue full")

const (
	TaskStatusWaiting  TaskStatus = "waiting"
	TaskStatusRunning  TaskStatus = "running"
	TaskStatusComplete TaskStatus = "completed"
)

// QueuedTask wraps a task with metadata
type QueuedTask struct {
	ID        uuid.UUID                 // Unique identifier for the task
	Task      func(ctx context.Context) // The actual task (for simple functions)
	Status    TaskStatus
	QueuedAt  time.Time // When it was added to queue
	StartedAt time.Time // When it started running
}

func (q *Queue) Add(task func(ctx context.Context)) (uuid.UUID, error) {
	taskId := uuid.New()
	qt := QueuedTask{
		ID:       taskId,
		Task:     task,
		Status:   TaskStatusWaiting,
		QueuedAt: time.Now(),
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.countTaskStatus(TaskStatusWaiting) >= q.queueSize {
		return taskId, ErrQueueFull
	}

	q.tasks = append(q.tasks, &qt)
	q.cond.Signal()

	return taskId, nil
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
	//

	spew.Dump("tasks", q.tasks)

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

func (q *Queue) CancelJob() {
	//todo cancel running job
	// todo cancel pending job

}
