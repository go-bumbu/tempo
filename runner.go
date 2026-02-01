package tempo

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"sync"
)

// QueueRunner is a task runner that manages task execution with parallelism control
type QueueRunner struct {
	TaskQueue
	wg          sync.WaitGroup
	parallelism int

	// runtime context
	ctx    context.Context
	cancel context.CancelFunc

	// handle the clean shutdown
	stopOnce sync.Once
	stopChan chan struct{}
}

type RunnerCfg struct {
	Parallelism int
	QueueSize   int
	HistorySize int
}

// NewQueueRunner creates a new QueueRunner instance
func NewQueueRunner(cfg RunnerCfg) *QueueRunner {
	ctx, cancel := context.WithCancel(context.Background())

	rq := QueueRunner{
		TaskQueue: TaskQueue{
			mu:         sync.Mutex{},
			tasks:      []*QueuedTask{},
			maxWaiting: cfg.QueueSize,
			maxDone:    cfg.HistorySize,
		},
		parallelism: cfg.Parallelism,

		ctx:    ctx,
		cancel: cancel,

		stopChan: make(chan struct{}),
	}
	rq.cond = sync.NewCond(&rq.mu)
	return &rq
}

// StartBg begins processing tasks
func (r *QueueRunner) StartBg() {

	r.wg.Go(func() {
		<-r.ctx.Done()
		// Wake up waiting workers to shut down
		r.unlockAllWaiting()
	})

	// TODO add a ticker to call clean history

	// Fixed worker pool - each worker loops forever
	for i := 0; i < r.parallelism; i++ {
		r.wg.Go(func() {
			for {
				task, err := r.WaitAndClaimTask(r.ctx)
				if err != nil {
					return // Shutdown
				}

				func() {
					childCtx, taskCancel := context.WithCancel(r.ctx)
					defer func() {
						taskCancel()
						close(task.done)
						if recVal := recover(); recVal != nil {
							r.mu.Lock()
							task.Status = TaskStatusPanicked
							task.err = fmt.Errorf("task panicked: %v", recVal)
							r.mu.Unlock()
						}
					}()
					taskErr := task.Task(childCtx)
					r.mu.Lock()
					if err != nil {
						task.Status = TaskStatusFailed
						task.err = taskErr
					} else {
						task.Status = TaskStatusComplete

					}
					r.mu.Unlock()
				}()

			}
		})
	}
}

// todo add test
func (r *QueueRunner) Cancel(ctx context.Context, id uuid.UUID) error {
	task, err := r.GetTask(id)
	if err != nil {
		return err
	}

	// todo only call cancel if the task is running
	task.cancelFn()

	// Wait until task finishes OR caller's ctx times out
	select {
	case <-task.done:
		// Task stopped
		r.mu.Lock()
		task.Status = TaskStatusCanceled
		r.mu.Unlock()
		return nil

	case <-ctx.Done(): // timeout OR external cancel
		r.mu.Lock()
		task.Status = TaskStatusCanceled
		r.mu.Unlock()
		// Task didn't stop in time
		return fmt.Errorf("cancel timeout: %w", ctx.Err())
	}
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
