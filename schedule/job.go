package schedule

import (
	"context"
	"fmt"
	"log/slog"
	"slices"

	"github.com/google/uuid"
	"github.com/reugn/go-quartz/quartz"
)

// enqueueJob is the quartz.Job registered for a schedule: when its trigger
// fires, it enqueues the schedule's task on the runner.
//
// A fire that cannot be enqueued (a full queue, an unregistered task name) is
// logged and dropped. Returning the error is safe: go-quartz reschedules a job
// before executing it and defaults to zero retries, so an error neither
// unschedules the job nor causes a re-run.
type enqueueJob struct {
	schedID  uuid.UUID
	taskName string
	params   []byte
	enq      Enqueuer
	log      *slog.Logger
}

// Verify enqueueJob satisfies the quartz.Job interface.
var _ quartz.Job = (*enqueueJob)(nil)

func (j *enqueueJob) Execute(_ context.Context) error {
	// Clone: tempo does not copy the params slice, and this job hands the same
	// bytes out on every fire. A handler that mutated them would corrupt every
	// later run of this schedule.
	taskID, err := j.enq.AddRaw(j.taskName, slices.Clone(j.params))
	if err != nil {
		j.log.Warn("schedule fire could not be enqueued",
			slog.String("component", "tempo/schedule"),
			slog.String("scheduleId", j.schedID.String()),
			slog.String("task", j.taskName),
			slog.String("error", err.Error()))
		return err
	}
	j.log.Info("schedule fired",
		slog.String("component", "tempo/schedule"),
		slog.String("scheduleId", j.schedID.String()),
		slog.String("task", j.taskName),
		slog.String("taskId", taskID.String()))
	return nil
}

func (j *enqueueJob) Description() string {
	return fmt.Sprintf("tempo schedule %s: enqueue task %q", j.schedID, j.taskName)
}
