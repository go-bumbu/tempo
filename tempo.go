package tempo

import (
	"fmt"
	"github.com/robfig/cron/v3"
	"sync"
	"time"
)

type Task struct {
	ID      int
	Message string
	Run     func() // the actual work
}

type Runner struct {
	tasks chan Task
	wg    sync.WaitGroup
}

func New(workers int, queueSize int) *Runner {
	tr := &Runner{
		tasks: make(chan Task, queueSize),
	}

	// start worker background routine
	for i := 1; i <= workers; i++ {
		tr.wg.Add(1)
		go tr.worker(i)
	}

	return tr
}

// Stop closes the task channel and waits for workers to finish
func (tr *Runner) Stop() {
	close(tr.tasks)
	tr.wg.Wait()
}

func (tr *Runner) worker(id int) {
	defer tr.wg.Done()
	for task := range tr.tasks {
		fmt.Printf("Worker %d executing task %d: %s\n", id, task.ID, task.Message)
		task.Run() // execute the actual task
	}
}

// OneShot enqueues a task for execution
func (tr *Runner) OneShot(task Task) {
	tr.tasks <- task
}

func (tr *Runner) Schedule(task Task, cronStr string) {
	c := cron.New(cron.WithSeconds()) // support seconds if needed

	// Run every day at 08:00
	c.AddFunc(cronStr, func() {
		tr.OneShot(task)
	})

	c.Start()
	defer c.Stop()
}

// Interval will run the task continuously with a pause in between of pause duration
func (tr *Runner) Interval(pause time.Duration, task Task) {

}
