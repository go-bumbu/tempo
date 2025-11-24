package tempo

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"sync"
	"time"
)

type Worker struct {


	ctx           context.Context
	cancel        context.CancelFunc
	cancelTimeout time.Duration
	workerGroup   sync.WaitGroup

	taskQueues map[string]*queue
}

func NewTaskRunner() *Worker {
	ctx, cancel := context.WithCancel(context.Background())



	p := &Worker{

		ctx:           ctx,
		cancel:        cancel,
		cancelTimeout: time.Second,
		workerGroup:   sync.WaitGroup{},

		taskQueues:    make(map[string]*queue),
	}
	return p
}

func (p *Worker) Start() {
	for {
		select {
			case
			default:

		}
	}


}
func (p *Worker) Wait() {

}
func (p *Worker) ShutDown() error {
	return nil
}

func (p *Worker) EnQueue(fn func()) error {
	_, err := p.AddTask("queueName", fn)
	if err != nil{
		return err
	}
	return nil
}

func (p *Worker) worker(fn func()) error {
	defer p.workerGroup.Done()
	for {
		select {
		case task, ok := <-p.tasksChan:
			if !ok {
				return nil// Channel closed, no more tasksChan
			}
			fn()
		case <-p.ctx.Done():
			return // Exit when context is cancelled
		}
	}
}



// ================================================00000
// integrating queue into task handler

func (h *Worker) getQueue(name string) (*queue, error) {
	got, ok := h.taskQueues[name]
	if !ok {
		return nil, fmt.Errorf("queue %s not found", name)
	}
	return got, nil
}

// RegisterQueue adds a new queue with its own parallelism settings
func (h *Worker) RegisterQueue(name string, maxParallelism, queueSize int) {
	h.taskQueues[name] = &queue{
		name:           name,
		maxParallelism: maxParallelism,
		queueSize:      queueSize,
		tasks:          make([]*QueuedTask, 0, queueSize),
	}
}

// AddTask adds a task to the waiting queue
func (h *Worker) AddTask(queueName string, task func()) (string, error) {

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

func (h *Worker) StarNextTask(queueName string) error {
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