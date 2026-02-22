package tempo

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
)

// QueueRunner runs tasks from a TaskQueue by pulling the next task from the queue and the function from its registry.
type QueueRunner struct {
	queue        *TaskQueue
	registry     *taskRegistry
	parallelism  int
	historySize  int
	cleanupTimer time.Duration

	logSink    TaskLogSink
	logLevel   slog.Level
	taskLogger *slog.Logger // shared when LogSink set; handler reads task ID from context

	ctx    context.Context
	cancel context.CancelFunc

	stopOnce  sync.Once
	startDone chan struct{} // closed when StartBg has finished adding goroutines; ShutDown waits on it
	stopChan  chan struct{}
	wg        sync.WaitGroup

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
	// Persistence mirrors task state; must not be nil.
	Persistence TaskStatePersistence
	// LogSink, when set, receives task log lines. Tasks use tempo.Logger(ctx).InfoContext(ctx, "msg").
	LogSink TaskLogSink
	// LogLevel is the minimum slog level sent to LogSink (e.g. slog.LevelInfo). Zero is Info.
	LogLevel slog.Level
}

// NewQueueRunner creates a QueueRunner with an internal queue built from cfg. Use RegisterTask to add task definitions. cfg.Persistence must not be nil.
func NewQueueRunner(cfg RunnerCfg) (*QueueRunner, error) {
	if cfg.Persistence == nil {
		return nil, errors.New("tempo: persistence must not be nil")
	}
	ctx, cancel := context.WithCancel(context.Background())
	if cfg.CleanupTimer == 0 {
		cfg.CleanupTimer = 5 * time.Minute
	}
	if cfg.HistorySize == 0 {
		cfg.HistorySize = 10
	}
	queueCfg := TaskQueueCfg{
		QueueSize:   cfg.QueueSize,
		HistorySize: cfg.HistorySize,
		Persistence: cfg.Persistence,
	}
	queue := NewTaskQueue(queueCfg)
	reg := newTaskRegistry()
	r := &QueueRunner{
		queue:        queue,
		registry:     reg,
		parallelism:  cfg.Parallelism,
		historySize:  cfg.HistorySize,
		cleanupTimer: cfg.CleanupTimer,
		logSink:      cfg.LogSink,
		logLevel:     cfg.LogLevel,
		ctx:          ctx,
		cancel:       cancel,
		startDone:    make(chan struct{}),
		stopChan:     make(chan struct{}),
		running:      make(map[uuid.UUID]runState),
		runningCount: make(map[string]int),
	}
	if cfg.LogSink != nil {
		r.taskLogger = slog.New(NewSinkHandler(cfg.LogSink, cfg.LogLevel))
	}
	return r, nil
}

// RegisterTask registers a task definition (overwrites if present). Def.Name is the task name.
func (r *QueueRunner) RegisterTask(def TaskDef) {
	r.registry.add(def)
}

// StartBg begins processing tasks from the store.
// The wait group count is added upfront so ShutDown can safely call Wait without racing with Add.
// ShutDown must not be called until StartBg has returned (startDone enforces this).
func (r *QueueRunner) StartBg() {
	r.wg.Add(1 + r.parallelism)
	defer close(r.startDone)

	go func() {
		defer r.wg.Done()
		<-r.ctx.Done()
		r.queue.UnblockAll()
	}()

	go r.autoClean()

	for i := 0; i < r.parallelism; i++ {
		go func() {
			defer r.wg.Done()
			for {
				canClaim := r.buildCanClaim()
				id, name, err := r.queue.NextTask(r.ctx, canClaim)
				if err != nil {
					return
				}

				r.runMu.Lock()
				r.runningCount[name]++
				r.runMu.Unlock()

				def, ok := r.registry.lookup(name)
				if !ok {
					_ = r.queue.SetStatus(context.Background(), id, TaskStatusFailed, time.Time{}, time.Now())
					r.decrRunningCount(name)
					continue
				}

				childCtx, taskCancel := context.WithCancel(r.ctx)
				if r.logSink != nil {
					childCtx = context.WithValue(childCtx, taskIDKey, id)
					childCtx = context.WithValue(childCtx, taskLoggerKey, r.taskLogger)
					r.appendTaskLog(childCtx, id, "INFO", "task started")
				}
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
							r.appendTaskLog(childCtx, id, "ERROR", fmt.Sprint(recVal))
						}
					}()
					taskErr := def.Run(childCtx)
					finalEndedAt = time.Now()
					if taskErr == nil {
						finalStatus = TaskStatusComplete
						r.appendTaskLog(childCtx, id, "INFO", "task finished")
					} else if errors.Is(taskErr, context.Canceled) {
						finalStatus = TaskStatusCanceled
						r.appendTaskLog(childCtx, id, "INFO", "task canceled")
					} else {
						finalStatus = TaskStatusFailed
						r.appendTaskLog(childCtx, id, "ERROR", taskErr.Error())
					}
				}()

				_ = r.queue.SetStatus(context.Background(), id, finalStatus, time.Time{}, finalEndedAt)
			}
		}()
	}
}

// appendTaskLog sends a log line to the sink when configured; errors are ignored.
func (r *QueueRunner) appendTaskLog(ctx context.Context, id uuid.UUID, level string, msg string) {
	if r.logSink != nil {
		_ = r.logSink.Append(ctx, id, level, msg)
	}
}

func (r *QueueRunner) buildCanClaim() func(name string) bool {
	return func(name string) bool {
		def, ok := r.registry.lookup(name)
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
// StartBg must have been called first; ShutDown waits for StartBg to finish before proceeding.
func (r *QueueRunner) ShutDown(ctx context.Context) error {
	var err error
	r.stopOnce.Do(func() {
		<-r.startDone // ensure StartBg has completed (wg.Add done) before we Wait
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
