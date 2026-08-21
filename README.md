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
tempo.Register(runner, "scan", func(ctx context.Context, p ScanParams) error {
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
