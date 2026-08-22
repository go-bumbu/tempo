# Tempo

Tempo is a lightweight background job runner and task queue library for Go. It provides a simple API to manage concurrent task execution with built-in support for graceful shutdown and task lifecycle management.

## Features

- **Task Queue Management** - Add tasks to a queue with configurable maximum size
- **Parallelism Control** - Limit the number of concurrent task executions
- **Task Status Tracking** - Query task status (waiting, running, complete, failed, panicked, canceled)
- **Task Cancellation** - Cancel running or pending tasks with timeout support
- **Graceful Shutdown** - Clean shutdown that waits for running tasks to complete
- **Task History** - Automatic cleanup of completed task history with configurable retention

## Installation

```bash
go get github.com/go-bumbu/tempo
```

## Quick Start

```go
runner, err := tempo.NewQueueRunner(tempo.RunnerCfg{
    Parallelism: 2, QueueSize: 10, HistorySize: 10,
    Persistence: tempo.NewMemPersistence(),
})
if err != nil {
    panic(err)
}

type ScanParams struct {
    Mode string `json:"mode"` // "normal" | "full"
}

// register a typed task
tempo.Register(runner, "scan", func(ctx context.Context, log *slog.Logger, p ScanParams) error {
    fmt.Printf("scan mode=%s\n", p.Mode)
    return nil
})

runner.StartBg()

// enqueue with typed params
if _, err := tempo.Enqueue(runner, "scan", ScanParams{Mode: "full"}); err != nil {
    panic(err)
}

// or enqueue by name with a raw JSON payload (e.g. from an HTTP handler)
if _, err := runner.AddRaw("scan", []byte(`{"mode":"normal"}`)); err != nil {
    panic(err)
}

if err := runner.ShutDown(context.TODO()); err != nil {
    panic(err)
}
```

## Configuration

```go
tempo.RunnerCfg{
    Parallelism:  4,              // Number of concurrent workers (required)
    QueueSize:    100,            // Maximum pending tasks in queue
    HistorySize:  50,             // Number of completed tasks to retain
    CleanupTimer: 5 * time.Minute, // Interval for history cleanup (default: 5min)
}
```

## Persistence

By default task state is kept in memory (`tempo.NewMemPersistence()`) and is lost
on restart. For durable state, pass a `dbqueue` store — a gorm-backed
`RecoverablePersistence`, kept out of the core package so gorm is not forced on
callers who do not need it:

```go
store, err := dbqueue.New(db) // AutoMigrates the tempo_tasks table
if err != nil {
    panic(err)
}
runner, err := tempo.NewQueueRunner(tempo.RunnerCfg{
    Parallelism: 2, QueueSize: 10, Persistence: store,
})
```

On startup the runner reloads persisted tasks: a task still **waiting** when the
process died resumes and runs, while one caught **running** is reconciled to
**failed** — no worker owns it, and re-running could repeat side effects. The
in-memory persistence has no `List`, so it recovers nothing.

## Scheduling

`tempo/schedule` runs tasks on a cron timetable. Schedules are persisted and can
be edited while the process runs; the scheduler owns the write path, so a change
is stored and rescheduled in one call.

```go
sched, err := schedule.New(schedule.Cfg{
    Store:    schedule.NewMemStore(), // or dbschedule.New(db)
    Enqueuer: runner,                 // *tempo.QueueRunner satisfies this directly
})
if err != nil {
    panic(err)
}
if err := sched.Start(ctx); err != nil {
    panic(err)
}

// One task, two cadences, different parameters.
nightly, err := sched.Create(ctx, schedule.Schedule{
    TaskName: "scan",
    Cron:     "0 2 * * *", // 5-field Unix or 6-field Quartz cron
    Params:   []byte(`{"full":false}`),
    Enabled:  true,
})
```

Editing at runtime — each call persists **and** reschedules:

```go
sched.Update(ctx, updated)              // new cron or params
sched.SetEnabled(ctx, nightly.ID, false) // pause without deleting
sched.Delete(ctx, nightly.ID)
sched.Trigger(ctx, nightly.ID)           // run now, with the stored params
sched.Reload(ctx)                        // re-sync after a restore wrote to the store
```

Validate user input before storing it:

```go
if err := schedule.ValidateCron(userInput); err != nil {
    // reject the request
}
```

Schedules persist across restarts with `dbschedule`:

```go
store, err := dbschedule.New(db) // AutoMigrates the tempo_schedules table
```

A fire that cannot be enqueued — a full queue, an unregistered task name — is
logged and dropped. Fires missed while the process was down are not replayed,
and a fire is enqueued even if the previous run is still going; use
`tempo.WithMaxParallelism(1)` to stop a task running concurrently with itself.

## Task logs

Each task handler receives a `*slog.Logger` as its second argument. Anything it
logs is routed to the runner's configured `LogSink`, tagged with the task id:

    r.RegisterRaw("resize", func(ctx context.Context, log *slog.Logger, params []byte) error {
        log.Info("started", "bytes", len(params))
        return nil
    })

Configure a sink on the runner with `RunnerCfg.LogSink` and the minimum level
with `RunnerCfg.LogLevel`. With no sink, the logger discards. Lifecycle lines
("task started/finished/canceled") are always recorded regardless of `LogLevel`.

A sink implements `TaskLogSink` (write). It may also implement `TaskLogReader`
(`Logs`) to read a task's lines back, and `TaskLogCleaner` (`RemoveTasks` /
`RetainOnly`) so the runner can reap them. Two built-in sinks:

- `tempo.MemTaskLogSink` — in-memory; implements all three.
- `filelog.New(filelog.Config{Dir: "..."})` — one JSON-Lines file per task on
  disk; implements all three.

### Retention

Logs live exactly as long as their task: when the runner trims a task from
history it removes that task's logs, and at startup it sweeps orphaned logs for
tasks it no longer knows about. There is no separate TTL.

Because `filelog` files outlive the process, pair it with a
`RecoverablePersistence` such as `dbqueue` if you want logs to survive restarts.
With the default in-memory persistence, the startup sweep deletes every prior
log file, since no task state remains to correlate them with.

## How To

### Handle Shutdown in Long-Running Tasks

For long-running tasks, check the context to respond to shutdown signals and allow for clean termination:

```go
myTask := func(ctx context.Context) error {
    ticker := time.NewTicker(1 * time.Second)
    defer ticker.Stop()
    
    for {
        select {
        case <-ctx.Done():
            fmt.Println("Shutdown received, cleaning up...")
            cleanup()
            return nil
            
        case <-ticker.C:
            // Do periodic work
            doWork()
        }
    }
}
```

### Query Task Status

```go
// List all tasks
tasks := runner.List()
for _, task := range tasks {
    fmt.Printf("Task %s: %s (queued: %v, started: %v)\n", 
        task.Name, task.Status.Str(), task.QueuedAt, task.StartedAt)
}

// Get specific task
task, err := runner.GetTask(taskID)
if err != nil {
    fmt.Printf("Task not found: %v\n", err)
}
```
