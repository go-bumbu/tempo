# Scheduling

`github.com/go-bumbu/tempo/schedule` keeps a persisted, runtime-editable
timetable and calls `AddRaw` on the runner when a schedule's cron expression
fires. It never runs tasks itself. `dbschedule` stores the timetable in SQL.

## Wiring

```go
store, err := dbschedule.New(db) // or schedule.NewMemStore(), lost on restart
if err != nil {
	return err
}
sched, err := schedule.New(schedule.Cfg{
	Store:    store,
	Enqueuer: runner, // *tempo.QueueRunner satisfies schedule.Enqueuer
	Location: loc,    // one time zone for every schedule; nil is time.Local
	Logger:   logger, // fire, skip and failure messages; nil is slog.Default()
})
if err != nil {
	return err
}
if err := sched.Start(ctx); err != nil { // ctx bounds only the initial read of the store
	return err
}
```

Set the runner up first (handlers registered, `StartBg` called), and on
shutdown stop the scheduler before the runner.

- `Start` loads the store and starts firing. It fails when the store cannot be
  read, and a failed `Start` can be retried. A schedule whose cron no longer
  parses is skipped with a warning. Calling `Start` again after it succeeded
  does nothing.
- `ShutDown(ctx)` stops firing and waits for in-flight fires. It returns
  `schedule.ErrUnsafeStop` when ctx ends first. `Wait()` blocks until it has
  finished.

## A schedule

```go
type Schedule struct {
	ID        uuid.UUID
	TaskName  string          // the task enqueued on every fire
	Cron      string          // 5-field Unix or 6-field Quartz
	Params    json.RawMessage // handed to the task verbatim on every fire
	Enabled   bool
	CreatedAt time.Time
	UpdatedAt time.Time
}
```

Several schedules may share a `TaskName`, which gives one task several
cadences with different params. `List` and `Get` return `ScheduleInfo`, which
adds `NextFireAt` (nil when disabled or not registered). An enabled schedule
with a nil `NextFireAt` has a cron that no longer parses; save it again to fix
it.

## Operations

| Call | Notes |
|---|---|
| `Create(ctx, s)` | Validates the cron and requires `TaskName`. Generates the id when it is `uuid.Nil`; honours a supplied id, returning `ErrScheduleExists` when it is taken. Stores the normalized 6-field cron. |
| `Update(ctx, s)` | Replaces the whole schedule identified by `s.ID`, keeping `CreatedAt`. Fields left zero are stored zero, so an `Enabled` you forgot to set disables the schedule. Read, modify, write. |
| `SetEnabled(ctx, id, on)` | Pauses or resumes without touching anything else. |
| `Delete(ctx, id)` | Removes the schedule and its job. |
| `Trigger(ctx, id)` | Enqueues the schedule's task now with its stored params, enabled or not. Works before `Start` and after `ShutDown`. Returns the task id; for a singleton task, the id of the task already in flight. |
| `List(ctx)`, `Get(ctx, id)` | Read, with `NextFireAt`. There is no lookup by task name: filter `List` on `TaskName`. |
| `Reload(ctx)` | Rebuilds the jobs from the store after something else wrote to it (a restore, manual SQL). |

`Create`, `Update`, `SetEnabled` and `Delete` return `ErrNotStarted` before
`Start`. Each one persists and reschedules in one call, so the store and the
running jobs stay in step.

```go
// Change a schedule's cron: read, modify, write.
info, err := sched.Get(ctx, id)
if err != nil {
	return err
}
s := info.Schedule
s.Cron = "30 4 * * *"
if _, err := sched.Update(ctx, s); err != nil {
	return err
}
```

To provision schedules idempotently, for instance one nightly schedule per
entity, derive the id from the entity:

```go
id := uuid.NewSHA1(uuid.NameSpaceURL, []byte("myapp/feeds/"+feedID+"/nightly"))
_, err := sched.Create(ctx, schedule.Schedule{
	ID:       id,
	TaskName: feedTaskName(feedID),
	Cron:     "0 3 * * *",
	Enabled:  true,
})
if err != nil && !errors.Is(err, schedule.ErrScheduleExists) {
	return err
}
```

## Cron expressions

- Both forms are accepted: 5-field Unix (`min hour dom month dow`) and 6-field
  Quartz, which has seconds first. What is stored, and returned, is the 6-field
  form: `0 2 * * *` comes back as `0 0 2 * * *`.
- In the 5-field form, day-of-week numbers are translated from Unix (0 or
  7 is Sunday) to Quartz (1 is Sunday). Quartz-only tokens (`L`, `5L`, `1#2`)
  are not translated, so in a 5-field expression they keep Quartz numbering:
  `0 3 * * 1#2` fires on the second Sunday. Write those in the 6-field form.
- Validate user input with `schedule.ValidateCron(expr)` before storing it.
  `Create` and `Update` validate too. `schedule.NormalizeCron(expr)` returns
  the stored form.

## Limits

- **One process per store.** Two schedulers on one `tempo_schedules` table
  each fire every enabled schedule, so a nightly job runs twice. `Create`'s
  existence check is only serialized within one process. There is no leader
  election: run one scheduling process per store, or give each process its
  own store.
- **One time zone for every schedule.** `Cfg.Location` applies to all of them;
  a schedule cannot carry its own zone. Nil means `time.Local`, the server's
  zone, which in a container is often UTC. When your users' wall clock is not
  the server's, set `Location` explicitly, for example from
  `time.LoadLocation("Europe/Zurich")`.
- **Missed fires are not replayed.** Fires due while the process was down, or
  before `Start`, are skipped; the next fire is computed from now. For catch-up,
  check at startup when the task last ran and call `Trigger`.
- **A fire does not wait for the previous run.** Each fire enqueues a new task,
  even while the last one is still waiting or running. To avoid overlapping
  runs, register the task with `WithMaxParallelism(1)` (fires queue behind the
  current run) or `WithSingleton()` (the fire is skipped).
- **A coalesced fire is skipped.** With `WithSingleton()`, a fire that finds
  the task in flight is logged at Info as skipped and dropped. It does not run
  later.
- **A fire that cannot be enqueued is dropped.** On `ErrQueueFull` the fire is
  logged at Warn and not retried.
- **A fire for a name with no handler** is enqueued, and the task fails. Delete
  an entity's schedules before you unregister its task.

## `dbschedule`

`dbschedule.New(db *gorm.DB)` runs `AutoMigrate` for `tempo_schedules` and is
safe to call more than once. The table name is fixed. There is no unique index
on `task_name`, because several schedules may drive one task.
