// Package schedule adds cron scheduling to tempo: a persisted, runtime-editable
// timetable that enqueues tempo tasks when their cron expression fires.
//
// The Scheduler owns the write path: Create, Update, SetEnabled and Delete each
// persist the change and reschedule the affected job in a single call, so a
// caller cannot leave the store and the running scheduler out of step. Use
// Reload when something else writes to the store (a backup restore, manual SQL).
//
// # One process per store
//
// A Scheduler assumes it is the only one running against its store. Two
// processes sharing a single tempo_schedules table each run their own quartz
// instance and both fire every enabled schedule, so a nightly job runs twice.
// Create's existence check is likewise serialized only by this Scheduler's
// in-process mutex, not by a database transaction, so two processes can race
// past it. There is no leader election here: run exactly one scheduling process
// per store, or partition the schedules into a store per process.
package schedule

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	// ErrScheduleNotFound is returned when no schedule exists for an id.
	ErrScheduleNotFound = errors.New("schedule not found")
	// ErrScheduleExists is returned by Create when the supplied id is already taken.
	ErrScheduleExists = errors.New("schedule already exists")
	// ErrNotStarted is returned by write methods called before Start.
	ErrNotStarted = errors.New("scheduler not started")
	// ErrUnsafeStop is returned when ShutDown's context expires before all
	// in-flight fires have returned.
	ErrUnsafeStop = errors.New("unsafe stop: scheduler did not shut down in time")
)

// Schedule is one cron timetable entry: run TaskName with Params whenever Cron
// fires. Several schedules may share a TaskName, so one task can have both a
// daily and a weekly cadence with different parameters.
type Schedule struct {
	ID       uuid.UUID `json:"id"`
	TaskName string    `json:"task_name"`
	Cron     string    `json:"cron"`
	// Params is the payload handed to the task, verbatim, on every fire. It is
	// json.RawMessage rather than []byte so it embeds in JSON instead of
	// base64-encoding itself.
	//
	// Create and Update take their own copy, and every fire hands the task a
	// fresh one, so the caller keeps ownership of the slice it passes and may
	// reuse or mutate it afterwards without affecting a stored schedule.
	Params    json.RawMessage `json:"params,omitempty"`
	Enabled   bool            `json:"enabled"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

// ScheduleInfo is a Schedule plus the one thing a caller cannot derive itself.
//
// An Enabled schedule with a nil NextFireAt is not running. That is the only
// signal available for a row Reload warn-skipped because its cron no longer
// parses: the store still says Enabled: true, since Reload does not write, while
// List and Get report no next fire time because no job was registered. Treat the
// combination as "broken cron" and re-save the schedule to clear it.
type ScheduleInfo struct {
	Schedule
	// NextFireAt is read from the live trigger. It is nil — and omitted from the
	// JSON — when the schedule is disabled or not currently registered.
	NextFireAt *time.Time `json:"next_fire_at,omitempty"`
}

// Enqueuer is how a fire reaches the task queue. *tempo.QueueRunner satisfies
// this interface as-is; no adapter is needed.
type Enqueuer interface {
	AddRaw(name string, params []byte) (uuid.UUID, error)
}
