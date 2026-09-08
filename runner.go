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

	logSink      TaskLogSink
	progressSink TaskProgressSink
	logLevel     slog.Level

	ctx    context.Context
	cancel context.CancelFunc

	stopOnce  sync.Once
	startDone chan struct{} // closed when StartBg has finished adding goroutines; ShutDown waits on it
	stopChan  chan struct{}
	wg        sync.WaitGroup

	runMu   sync.Mutex
	running map[uuid.UUID]runState
	limiter *limiter
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
	// LogSink, when set, receives task log lines. Each task handler is given a *slog.Logger to write them.
	LogSink TaskLogSink
	// LogLevel is the minimum slog level sent to LogSink (e.g. slog.LevelInfo). Zero is Info.
	LogLevel slog.Level
	// ProgressSink, when set, receives task progress updates. Each task handler is given a Progress reporter to publish them.
	ProgressSink TaskProgressSink
}

// NewQueueRunner creates a QueueRunner with an internal queue built from cfg. Use RegisterRaw or Register to add task definitions. cfg.Persistence must not be nil.
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
		progressSink: cfg.ProgressSink,
		logLevel:     cfg.LogLevel,
		ctx:          ctx,
		cancel:       cancel,
		startDone:    make(chan struct{}),
		stopChan:     make(chan struct{}),
		running:      make(map[uuid.UUID]runState),
		limiter:      newLimiter(),
	}

	list, _ := queue.List(context.Background())
	ids := make([]uuid.UUID, 0, len(list))
	for _, info := range list {
		ids = append(ids, info.ID)
	}
	if c, ok := cfg.LogSink.(TaskLogCleaner); ok {
		_ = c.RetainOnly(context.Background(), ids)
	}
	if c, ok := cfg.ProgressSink.(TaskProgressCleaner); ok {
		_ = c.RetainOnly(context.Background(), ids)
	}
	return r, nil
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
				canClaim, release := r.buildClaim()
				id, name, params, err := r.queue.NextTask(r.ctx, canClaim)
				if err != nil {
					return
				}

				entry, ok := r.registry.lookup(name)
				if !ok {
					_ = r.queue.SetStatus(context.Background(), id, TaskStatusFailed, time.Time{}, time.Now())
					release()
					continue
				}

				childCtx, taskCancel := context.WithCancel(r.ctx)
				// Record the cancelable run-state before the "task started" log: a
				// slow LogSink can park the worker in that log call while the queue
				// already shows the task Running. A Cancel arriving in that window
				// must find the task here, not see Running in the queue and wrongly
				// report it absent from the runner.
				done := make(chan struct{})
				r.runMu.Lock()
				r.running[id] = runState{cancel: taskCancel, done: done}
				r.runMu.Unlock()
				var taskLog *slog.Logger
				if r.logSink != nil {
					taskLog = slog.New(newSinkHandler(r.logSink, r.logLevel, id))
				} else {
					taskLog = discardLogger
				}
				var taskProg Progress
				if r.progressSink != nil {
					taskProg = newSinkReporter(r.progressSink, id)
				} else {
					taskProg = discardProgress
				}
				r.appendTaskLog(childCtx, id, "INFO", "task started")

				var finalStatus TaskStatus
				var finalEndedAt time.Time
				func() {
					defer func() {
						taskCancel()
						close(done)
						r.runMu.Lock()
						delete(r.running, id)
						r.runMu.Unlock()
						release()
						if recVal := recover(); recVal != nil {
							finalStatus = TaskStatusPanicked
							finalEndedAt = time.Now()
							r.appendTaskLog(childCtx, id, "ERROR", fmt.Sprint(recVal))
						}
					}()
					taskErr := entry.run(childCtx, taskLog, taskProg, params)
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

// buildClaim returns the claim gate NextTask calls (while holding the queue
// lock) to decide whether a waiting task may run, plus a release func to free
// the reservation once the task finishes. The gate reserves the task's slots —
// its per-name slot and, if it was registered with WithExclusionGroup, its
// group slot — atomically via the limiter, so two workers can never both pass a
// limit before either records its claim. NextTask calls the gate at most once
// successfully per call, for the task it returns; release frees exactly that
// task's reservation and is a no-op (and idempotent) if nothing was claimed.
func (r *QueueRunner) buildClaim() (canClaim func(name string) bool, release func()) {
	var held func()
	canClaim = func(name string) bool {
		var nameLimit int
		var group string
		if entry, ok := r.registry.lookup(name); ok {
			nameLimit = entry.maxParallelism
			group = entry.group
		}
		rel, ok := r.limiter.tryAcquire(name, nameLimit, group)
		if !ok {
			return false
		}
		held = rel
		return true
	}
	release = func() {
		if held != nil {
			held()
		}
	}
	return canClaim, release
}

func (r *QueueRunner) autoClean() {
	ticker := time.NewTicker(r.cleanupTimer)
	for {
		select {
		case <-ticker.C:
			r.cleanupOnce(context.Background())
		case <-r.stopChan:
			return
		}
	}
}

// cleanupOnce trims task history and reaps the log files and progress records of the trimmed tasks.
func (r *QueueRunner) cleanupOnce(ctx context.Context) {
	removed, _ := r.queue.CleanHistory(ctx, r.historySize)
	if len(removed) == 0 {
		return
	}
	if c, ok := r.logSink.(TaskLogCleaner); ok {
		_ = c.RemoveTasks(ctx, removed)
	}
	if c, ok := r.progressSink.(TaskProgressCleaner); ok {
		_ = c.RemoveTasks(ctx, removed)
	}
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
