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
    // Create a runner with 2 parallel workers and queue size of 10
    runner := tempo.NewQueueRunner(tempo.RunnerCfg{
        Parallelism: 2, QueueSize:   10, HistorySize: 10,
    })

    // Start processing tasks in the background
    runner.StartBg()

    // Define a task
    myTask := func(ctx context.Context) error {
        fmt.Println("Executing task")
        time.Sleep(100 * time.Millisecond) // Simulate work
        return nil
    }

    // Add the task to the queue
    _, err := runner.Add(myTask, "my-task")
    if err != nil {
        fmt.Printf("Failed to add task: %v\n", err)
    }

    if err := runner.ShutDown(context.TODO()); err != nil {
        fmt.Printf("Shutdown error: %v\n", err)
    }
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
