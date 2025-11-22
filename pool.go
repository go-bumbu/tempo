package tempo

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Task type
type Task interface {
	Run(ctx context.Context) error
}

// WorkerPool with context-based cancellation
type WorkerPool struct {
	tasksChan     chan Task
	poolSize      int
	ctx           context.Context
	cancel        context.CancelFunc
	cancelTimeout time.Duration
	workerGroup   sync.WaitGroup

	stopOnce sync.Once
	stopChan chan struct{}
}

type workerPoolOpts struct {
	poolSize        int
	queueSize       int
	shutdownTimeout time.Duration
}

// ======== Public API ========

// WorkerOption configures the Runner.
type WorkerOption func(*workerPoolOpts)

func WithPoolSize(n int) WorkerOption {
	return func(r *workerPoolOpts) {
		r.poolSize = n
	}
}

func WithQueueSize(n int) WorkerOption {
	return func(r *workerPoolOpts) {
		r.queueSize = n
	}
}

func WithQShutDownTimeout(d time.Duration) WorkerOption {
	return func(r *workerPoolOpts) {
		r.shutdownTimeout = d
	}
}

// NewWorkerPool creates a new pool with N poolSize and a task channel buffer of size bufSize
func NewWorkerPool(userOpts ...WorkerOption) *WorkerPool {
	// modify user options
	opts := workerPoolOpts{
		poolSize:        1,
		queueSize:       1,
		shutdownTimeout: 5 * time.Second,
	}
	for _, opt := range userOpts {
		opt(&opts)
	}

	ctx, cancel := context.WithCancel(context.Background())
	p := &WorkerPool{
		tasksChan:     make(chan Task, opts.queueSize),
		poolSize:      opts.poolSize,
		ctx:           ctx,
		cancel:        cancel,
		cancelTimeout: opts.shutdownTimeout,
		workerGroup:   sync.WaitGroup{},
		stopChan:      make(chan struct{}),
	}
	return p
}
func (p *WorkerPool) Start() {
	for i := 0; i < p.poolSize; i++ {
		p.workerGroup.Add(1)
		go p.worker()
	}
}

func (p *WorkerPool) Wait() {
	<-p.stopChan
}

var ErrUnsafeStop = errors.New("unsafe stop: some workers failed to shutdown")

// ShutDown gracefully cancels the context, stopping all poolSize
func (p *WorkerPool) ShutDown() error {

	// Unblock StartWait() if it's waiting
	p.stopOnce.Do(func() {
		close(p.stopChan)
	})

	close(p.tasksChan) // stop accepting new tasksChan
	// ShutDown new jobs + notify running jobs
	p.cancel()

	// Wait with timeout
	ch := make(chan struct{})
	go func() {
		p.workerGroup.Wait()
		close(ch)
	}()

	select {
	case <-ch:
		return nil
	case <-time.After(p.cancelTimeout):
		return ErrUnsafeStop
	}
}

// worker runs a single worker loop
func (p *WorkerPool) worker() {
	defer p.workerGroup.Done()
	for {
		select {
		case task, ok := <-p.tasksChan:
			if !ok {
				return // Channel closed, no more tasksChan
			}
			_ = task.Run(p.ctx)
		case <-p.ctx.Done():
			return // Exit when context is cancelled
		}
	}
}

var ErrQueueFull = errors.New("queue full")

// EnQueue tries to enqueue a task without blocking
func (p *WorkerPool) EnQueue(task Task) error {
	select {
	case p.tasksChan <- task:
		return nil
	default:
		return ErrQueueFull
	}
}
