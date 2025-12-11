package tempo

import (
	"context"
	"errors"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/google/uuid"
	"sync"
)

// QueueRunner is a task runner that manages task execution with parallelism control
type QueueRunner struct {
	queue *Queue

	wg sync.WaitGroup

	// runtime context
	ctx    context.Context
	cancel context.CancelFunc

	// handle the clean shutdown
	stopOnce sync.Once
	stopChan chan struct{}
}

// NewQueueRunner creates a new QueueRunner instance
func NewQueueRunner(cfg QueueCfg) *QueueRunner {
	q := NewQueue(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	return &QueueRunner{
		queue: q,

		ctx:    ctx,
		cancel: cancel,

		stopChan: make(chan struct{}),
	}
}

// StartBg begins processing tasks
func (r *QueueRunner) StartBg() {

	//tasksCh := make(chan struct{}, r.queue.maxRunning)

	r.wg.Go(func() {
		<-r.ctx.Done()
		// Wake up waiting workers to shutdown
		r.queue.UnlockAll()
	})

	// Fixed worker pool - each worker loops forever
	for i := 0; i < r.queue.maxRunning; i++ {
		r.wg.Go(func() {
			for {
				// Atomically wait and claim
				task, err := r.queue.WaitAndClaimTask(r.ctx)
				if err != nil {
					return // Shutdown
				}

				// Execute with panic recovery
				func() {
					defer func() {
						if v := recover(); v != nil {
							spew.Dump(v)
							// TODO: set task status to failed
							// Drop the reference to the function, so it can be GC'ed.
							// task.data.f = nil
							// Close the task "done" channel, so Wait() unblocks.
							//close(task.data.done)
						}
					}()
					task.Task(r.ctx)
					r.queue.SetStatus(task.ID, TaskStatusComplete)
				}()
			}
		})
	}
}

// StartBg begins processing tasks
func (r *QueueRunner) StartBg2() {

	tasksCh := make(chan struct{}, r.queue.maxRunning)

	r.wg.Go(func() {
		<-r.ctx.Done()
		// Wake up waitForTask() for the last time during shutdown
		r.queue.Unlock()
	})

	// Fixed worker pool - each worker loops forever

	// main control loop
	r.wg.Go(func() {
		defer close(tasksCh)

		for {
			err := r.queue.WaitForTask(r.ctx)
			if err != nil {
				return
			}

			select {
			case tasksCh <- struct{}{}:
				// Task sent, wait for the next one.
			case <-r.ctx.Done():
				return
			}
		}
	})

	// workers
	for range r.queue.maxRunning { // read from queue?
		r.wg.Go(func() {
			for range tasksCh {
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

					task, err := r.queue.StartTask()
					if err != nil {
						// Another worker got the task first (race condition) and there is no task to run
						// for now This is the best way to handle it - just continue to next signal
						// if you have a better idea please contribute
						if errors.Is(err, ErrTaskNotFound) {
							return
						}
						// this should never panic since StartTask() only returns one error
						panic(fmt.Errorf("failed to start task: %w", err))
					}

					// todo get error
					task.Task(r.ctx)
					r.queue.SetStatus(task.ID, TaskStatusComplete) // todo status failed ?
				}()
			}
		})
	}
}

// Add adds a new task to the runner
func (r *QueueRunner) Add(fn func(ctx context.Context)) (uuid.UUID, error) {
	return r.queue.Add(fn)
}

var ErrUnsafeStop = errors.New("unsafe stop: some workers failed to shutdown")

// ShutDown gracefully shuts down the runner
func (r *QueueRunner) ShutDown(ctx context.Context) error {
	var err error

	r.stopOnce.Do(func() {
		r.cancel() // notify running jobs to stop

		shutdownCh := make(chan struct{})
		go func() {
			r.wg.Wait()
			close(shutdownCh)
		}()

		select {
		case <-shutdownCh:
			err = nil
		case <-ctx.Done():
			err = ErrUnsafeStop
		}

		// Unblock Wait() if it's waiting
		close(r.stopChan)
	})
	return err
}

// Wait blocks until the runner has shut down
func (r *QueueRunner) Wait() {
	<-r.stopChan
}

// List returns information about all tasks
func (r *QueueRunner) List() []QueueTaskInfo {
	return r.queue.List()
}
