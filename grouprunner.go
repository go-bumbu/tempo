package tempo

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// GroupRunner runs multiple background tasks (each a TaskDef) with configuration abstracted.
// Create with NewGroupRunner(), add tasks with Add(name, def), then Run() (blocking) while
// another goroutine calls Stop(ctx) on signal. The queue is created when Run is called,
// with queue size and parallelism equal to the number of tasks added.
//
//	rg := tempo.NewGroupRunner()
//	rg.Add(tempo.TaskDef{
//		Name: "http",
//		Run:  func(ctx context.Context) error { return httpServer(ctx, port1) },
//	})
//	go func() {
//		sigCh := make(chan os.Signal, 1)
//		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
//		<-sigCh
//		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
//		defer cancel()
//		_ = rg.Stop(ctx)
//	}()
//	_ = rg.Run()
type GroupRunner struct {
	mu         sync.Mutex
	tasks      []TaskDef
	qr         *QueueRunner
	blockCh    chan struct{}
	stopResult chan error
	stopOnce   sync.Once
	taskErr    error // first independent task failure; set before auto-Stop is triggered
}

// NewGroupRunner creates a GroupRunner. Add tasks with Add; then Run creates the queue (sized to the number of tasks) and blocks until Stop is called.
func NewGroupRunner() *GroupRunner {
	return &GroupRunner{
		tasks:      nil,
		blockCh:    make(chan struct{}),
		stopResult: make(chan error, 1),
	}
}

// Add registers a task to run. It is enqueued when Run is called. The task runs until ctx is cancelled (e.g. when Stop is called). Def.Name must be set.
func (g *GroupRunner) Add(def TaskDef) {
	g.mu.Lock()
	defer g.mu.Unlock()
	def.MaxParallelism = 1 // GroupRunner only allows one execution
	g.tasks = append(g.tasks, def)
}

// Run creates the queue runner with queue size and parallelism equal to the number of tasks added, starts it, and blocks until Stop is called. Returns the error from Stop, or nil. Returns an error if no tasks were added.
func (g *GroupRunner) Run() error {
	g.mu.Lock()
	tasks := make([]TaskDef, len(g.tasks))
	copy(tasks, g.tasks)
	g.mu.Unlock()

	n := len(tasks)
	if n == 0 {
		return fmt.Errorf("tempo: no tasks added")
	}

	onFailOnce := &sync.Once{}
	for i := range tasks {
		original := tasks[i].Run
		tasks[i].Run = func(ctx context.Context) error {
			err := original(ctx)
			if err != nil && ctx.Err() == nil {
				onFailOnce.Do(func() {
					g.mu.Lock()
					g.taskErr = err
					g.mu.Unlock()
					go func() {
						stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
						defer cancel()
						_ = g.Stop(stopCtx)
					}()
				})
			}
			return err
		}
	}

	cfg := RunnerCfg{
		Parallelism: n,
		QueueSize:   n,
		HistorySize: n * 2,
		Persistence: NewMemPersistence(),
	}
	qr, err := NewQueueRunner(cfg)
	if err != nil {
		return err
	}

	g.mu.Lock()
	g.qr = qr
	g.mu.Unlock()

	for _, def := range tasks {
		qr.RegisterTask(def)
		if _, err := qr.Add(def.Name); err != nil {
			return err
		}
	}

	qr.StartBg()
	<-g.blockCh
	stopErr := <-g.stopResult
	g.mu.Lock()
	taskErr := g.taskErr
	g.mu.Unlock()
	if taskErr != nil {
		return taskErr
	}
	return stopErr
}

// Stop shuts down the queue runner with the given context (e.g. context.WithTimeout), then unblocks Run. Idempotent.
func (g *GroupRunner) Stop(ctx context.Context) error {
	var err error
	g.stopOnce.Do(func() {
		g.mu.Lock()
		qr := g.qr
		g.mu.Unlock()
		if qr != nil {
			err = qr.ShutDown(ctx)
		}
		g.stopResult <- err
		close(g.blockCh)
	})
	return err
}
