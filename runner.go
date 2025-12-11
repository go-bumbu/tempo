package tempo

import (
	"context"
	"errors"
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

	r.wg.Go(func() {
		<-r.ctx.Done()
		// Wake up waiting workers to shutdown
		r.queue.UnlockAll()
	})

	// Fixed worker pool - each worker loops forever
	for i := 0; i < r.queue.maxRunning; i++ {
		r.wg.Go(func() {
			for {
				task, err := r.queue.WaitAndClaimTask(r.ctx)
				if err != nil {
					return // Shutdown
				}

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
