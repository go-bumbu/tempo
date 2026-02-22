package tempo

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// taskLookupper is satisfied by *TaskRegistry (and any type with Lookup for resolving name -> TaskDef).
type taskLookupper interface {
	Lookup(name string) (TaskDef, bool)
}

// QueueRunner runs tasks from a TaskQueue by pulling the next task from the queue and the function from the registry.
type QueueRunner struct {
	queue        *TaskQueue
	lookup       taskLookupper
	parallelism  int
	historySize  int
	cleanupTimer time.Duration

	ctx    context.Context
	cancel context.CancelFunc

	stopOnce sync.Once
	stopChan chan struct{}
	wg       sync.WaitGroup

	runMu        sync.Mutex
	running      map[uuid.UUID]runState
	runningCount map[string]int
}

type runState struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// RunnerCfg holds configuration for the queue runner.
type RunnerCfg struct {
	Parallelism  int
	QueueSize    int
	HistorySize  int
	CleanupTimer time.Duration
}

// NewQueueRunner creates a QueueRunner that gets the next task from the queue and the function from the registry.
func NewQueueRunner(cfg RunnerCfg, queue *TaskQueue, registry taskLookupper) *QueueRunner {
	ctx, cancel := context.WithCancel(context.Background())
	if cfg.CleanupTimer == 0 {
		cfg.CleanupTimer = 5 * time.Minute
	}
	if cfg.HistorySize == 0 {
		cfg.HistorySize = 10
	}
	r := &QueueRunner{
		queue:        queue,
		lookup:       registry,
		parallelism:  cfg.Parallelism,
		historySize:  cfg.HistorySize,
		cleanupTimer: cfg.CleanupTimer,
		ctx:          ctx,
		cancel:       cancel,
		stopChan:     make(chan struct{}),
		running:      make(map[uuid.UUID]runState),
		runningCount: make(map[string]int),
	}
	return r
}

// StartBg begins processing tasks from the store.
func (r *QueueRunner) StartBg() {
	r.wg.Go(func() {
		<-r.ctx.Done()
		r.queue.UnblockAll()
	})

	go r.autoClean()

	for i := 0; i < r.parallelism; i++ {
		r.wg.Go(func() {
			for {
				canClaim := r.buildCanClaim()
				id, name, err := r.queue.NextTask(r.ctx, canClaim)
				if err != nil {
					return
				}

				r.runMu.Lock()
				r.runningCount[name]++
				r.runMu.Unlock()

				def, ok := r.lookup.Lookup(name)
				if !ok {
					_ = r.queue.SetStatus(context.Background(), id, TaskStatusFailed, time.Time{}, time.Now())
					r.decrRunningCount(name)
					continue
				}

				childCtx, taskCancel := context.WithCancel(r.ctx)
				done := make(chan struct{})
				r.runMu.Lock()
				r.running[id] = runState{cancel: taskCancel, done: done}
				r.runMu.Unlock()

				var finalStatus TaskStatus
				var finalEndedAt time.Time
				func() {
					defer func() {
						taskCancel()
						close(done)
						r.runMu.Lock()
						delete(r.running, id)
						r.runMu.Unlock()
						r.decrRunningCount(name)
						if recVal := recover(); recVal != nil {
							finalStatus = TaskStatusPanicked
							finalEndedAt = time.Now()
						}
					}()
					taskErr := def.Run(childCtx)
					finalEndedAt = time.Now()
					if taskErr != nil {
						if errors.Is(taskErr, context.Canceled) {
							finalStatus = TaskStatusCanceled
						} else {
							finalStatus = TaskStatusFailed
						}
					} else {
						finalStatus = TaskStatusComplete
					}
				}()

				_ = r.queue.SetStatus(context.Background(), id, finalStatus, time.Time{}, finalEndedAt)
			}
		})
	}
}

func (r *QueueRunner) buildCanClaim() func(name string) bool {
	return func(name string) bool {
		def, ok := r.lookup.Lookup(name)
		if !ok {
			return true
		}
		if def.MaxParallelism == 0 {
			return true
		}
		r.runMu.Lock()
		n := r.runningCount[name]
		r.runMu.Unlock()
		return n < def.MaxParallelism
	}
}

func (r *QueueRunner) decrRunningCount(name string) {
	r.runMu.Lock()
	r.runningCount[name]--
	if r.runningCount[name] <= 0 {
		delete(r.runningCount, name)
	}
	r.runMu.Unlock()
}

func (r *QueueRunner) autoClean() {
	ticker := time.NewTicker(r.cleanupTimer)
	for {
		select {
		case <-ticker.C:
			_ = r.queue.CleanHistory(context.Background(), r.historySize)
		case <-r.stopChan:
			return
		}
	}
}

// Add enqueues a task by name. The runner will look up the function from the registry when it runs.
func (r *QueueRunner) Add(name string) (uuid.UUID, error) {
	return r.queue.Add(name)
}

// List returns all tasks from the queue (e.g. for API display).
func (r *QueueRunner) List() []TaskInfo {
	list, err := r.queue.List(context.Background())
	if err != nil {
		return nil
	}
	return list
}

// GetTask returns task metadata by id. Returns ErrTaskNotFound if not found.
func (r *QueueRunner) GetTask(id uuid.UUID) (TaskInfo, error) {
	return r.queue.Get(context.Background(), id)
}

// Cancel cancels a waiting or running task.
func (r *QueueRunner) Cancel(ctx context.Context, id uuid.UUID) error {
	r.runMu.Lock()
	state, running := r.running[id]
	r.runMu.Unlock()

	if running {
		state.cancel()
		select {
		case <-state.done:
			_ = r.queue.SetStatus(context.Background(), id, TaskStatusCanceled, time.Time{}, time.Now())
			return nil
		case <-ctx.Done():
			_ = r.queue.SetStatus(context.Background(), id, TaskStatusCancelError, time.Time{}, time.Now())
			return fmt.Errorf("cancel timeout: %w", ctx.Err())
		}
	}

	info, err := r.queue.Get(ctx, id)
	if err != nil {
		return err
	}
	if info.Status == TaskStatusWaiting {
		return r.queue.SetStatus(ctx, id, TaskStatusCanceled, time.Time{}, time.Now())
	}
	if info.Status == TaskStatusRunning {
		return fmt.Errorf("task %s not found in runner", id)
	}
	return fmt.Errorf("task not cancelable: status %s", info.Status.Str())
}

var ErrUnsafeStop = errors.New("unsafe stop: some workers failed to shutdown")

// ShutDown gracefully shuts down the runner.
func (r *QueueRunner) ShutDown(ctx context.Context) error {
	var err error
	r.stopOnce.Do(func() {
		r.queue.UnblockAll()
		r.cancel()

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
		close(r.stopChan)
	})
	return err
}

// Wait blocks until the runner has shut down.
func (r *QueueRunner) Wait() {
	<-r.stopChan
}
