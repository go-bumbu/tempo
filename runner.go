package tempo

import (
	"context"
	"github.com/davecgh/go-spew/spew"
	"github.com/google/uuid"
	"sync"
)

// Runner is a task runner that manages task execution with parallelism control
type Runner struct {
	queue *TaskQueue

	wg sync.WaitGroup

	// runtime context
	ctx    context.Context
	cancel context.CancelFunc

	// handle the clean shutdown
	stopOnce sync.Once
	stopChan chan struct{}
}

// NewRunner creates a new Runner instance
func NewRunner(cfg TaskQueueCfg) *Runner {
	q := NewTaskQueue(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	return &Runner{
		queue: q,

		ctx:    ctx,
		cancel: cancel,

		stopChan: make(chan struct{}),
	}
}

// Run begins processing tasks
func (r *Runner) Run() {

	sem := make(chan struct{})

	r.wg.Go(func() {
		<-r.ctx.Done()
		// Wake up waitForTask() for the last time during shutdown
		r.queue.Unlock()
	})

	// main control loop
	r.wg.Go(func() {
		defer close(sem)

		for {
			err := r.queue.WaitForTask(r.ctx)
			if err != nil {
				return
			}
			select {
			case sem <- struct{}{}:
				// Task sent, wait for the next one.
			case <-r.ctx.Done():
				return
			}
		}
	})

	// workers
	for range r.queue.maxRunning { // read from queue?
		r.wg.Go(func() {
			for range sem {
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
					task, err := r.queue.StartWaiting()
					if err != nil {
						panic(err)
					}

					// todo get error
					task.Task(r.ctx)
					//task.data.err = task.data.f(ctx)
				}()
			}
		})
	}
}

// Add adds a new task to the runner
func (r *Runner) Add(fn func(ctx context.Context)) (uuid.UUID, error) {
	return r.queue.Add(fn)
}

// ShutDown gracefully shuts down the runner
func (r *Runner) ShutDown(ctx context.Context) error {
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
func (r *Runner) Wait() {
	<-r.stopChan
}

// List returns information about all tasks
func (r *Runner) List() []QueueTaskInfo {
	return r.queue.List()
}
