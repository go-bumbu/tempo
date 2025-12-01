package tempo

import (
	"context"
	"sync"
	"time"
)

//type Task struct {
//	ID      int
//	Message string
//	Run     func() // the actual work
//}

type JobHandler interface {
	Run(ctx context.Context) error
	Status() (JobStatus, string) // optional
}

type JobConfig struct {
	Name         string
	Schedule     Schedule   // e.g. cron or custom function
	Handler      JobHandler // the actual function to run
	AllowOverlap bool
	LastRun      time.Time
	Status       JobStatus
}
type JobStatus struct {
	LastRun   time.Time
	LastError error
	Runs      int
	Successes int
}

type Runner struct {
	tasks chan Task
	wg    sync.WaitGroup
	pool  *workerPool

	ctx    context.Context
	cancel context.CancelFunc
}

// Schedule interface avoids to hardcode a cron expression
// Then you can provide multiple implementations:
//   - CronSchedule — parses cron syntax.
//   - FixedInterval — simple every N minutes.
//   - CustomSchedule — user-provided logic.
//
// sample usage Schedule: tempo.Cron("@daily")
type Schedule interface {
	NextRun(after time.Time) time.Time
}

// ======== Public API ========

// Option configures the Runner.
type Option func(*Runner)

// WithMaxParallel sets the maximum number of concurrent jobs.
func WithMaxParallel(n int) Option {
	return func(r *Runner) {
		r.pool = newWorkerPool(n)
	}
}

const defaultMaxParallel = 1

func New(opts ...Option) *Runner {
	ctx, cancel := context.WithCancel(context.Background())
	r := &Runner{
		ctx:    ctx,
		cancel: cancel,
		pool:   newWorkerPool(defaultMaxParallel), // default

	}

	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Start begins job scheduling.
func (tr *Runner) Start() {
	//
	//for i := 1; i <= workers; i++ {
	//	tr.wg.Add(1)
	//	go tr.worker(i)
	//}
	//
	//for _, job := range r.jobs {
	//	r.wg.Add(1)
	//	go r.runJob(job)
	//}
}

func (tr *Runner) worker(id int) {
	//defer tr.wg.Done()
	//for task := range tr.tasks {
	//	fmt.Printf("Worker %d executing task %d: %s\n", id, task.ID, task.Message)
	//	task.Run() // execute the actual task
	//}
}

// Stop closes the task channel and waits for workers to finish
func (tr *Runner) Stop() {
	close(tr.tasks)
	tr.wg.Wait()

	tr.cancel()
	tr.wg.Wait()

}

//func (tr *Runner) worker(id int) {
//	defer tr.wg.Done()
//	for task := range tr.tasks {
//		fmt.Printf("Worker %d executing task %d: %s\n", id, task.ID, task.Message)
//		task.Run() // execute the actual task
//	}
//}

// OneShot enqueues a task for execution
func (tr *Runner) OneShot(task Task) {
	tr.tasks <- task
}

func (tr *Runner) Schedule(task Task, cronStr string) {
	//c := cron.New(cron.WithSeconds()) // support seconds if needed
	//
	//// Run every day at 08:00
	//c.AddFunc(cronStr, func() {
	//	tr.OneShot(task)
	//})
	//
	//c.Start()
	//defer c.Stop()
}

// Interval will run the task continuously with a pause in between of pause duration
func (tr *Runner) Interval(pause time.Duration, task Task) {

}

// ======== Worker Pool ========

type workerPool struct {
	sem chan struct{}
}

func newWorkerPool(n int) *workerPool {
	return &workerPool{sem: make(chan struct{}, n)}
}

func (p *workerPool) run(fn func()) {
	p.sem <- struct{}{}
	go func() {
		defer func() { <-p.sem }()
		fn()
	}()
}
