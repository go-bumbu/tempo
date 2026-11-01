# Tempo

Tempo is a lightweight background job runner for Go that runs inside your
process. You register named task handlers, enqueue tasks with typed or raw JSON
parameters, and a bounded pool of workers runs them. Tempo tracks every task's
status, lets you cancel tasks and shut down gracefully, and offers optional
durable state, per-task logs, progress reporting and cron scheduling.

## Capabilities

- **Bounded worker pool and queue.** `Parallelism` caps how many tasks run at
  once; `QueueSize` caps how many may wait.
- **Typed or raw parameters.** `Register` and `Enqueue` move a Go type as JSON;
  `RegisterRaw` and `AddRaw` take bytes, for example straight from an HTTP
  request.
- **Task lifecycle.** Each task is tracked as waiting, running, complete,
  failed, panicked, canceled or cancel_error. A bounded history of finished
  tasks is kept and trimmed automatically.
- **Cancel, unregister, shut down.** Cancel a single task, remove a handler
  along with its tasks, or stop the runner and wait for running tasks to
  return.
- **Per-task concurrency rules.** Cap a task's concurrent runs
  (`WithMaxParallelism`), drop duplicate enqueues while one is in flight
  (`WithSingleton`), or keep different tasks from running at the same time
  (`WithExclusionGroup`).
- **Persistence and crash recovery.** Task state lives in memory or in SQL
  (`dbqueue`). After a restart, waiting tasks resume and tasks that were
  interrupted are marked failed.
- **Per-task logs.** Each handler gets a `*slog.Logger` whose lines go to a
  pluggable sink: in memory, or one JSON Lines file per task (`filelog`). Logs
  are removed along with their task.
- **Progress reporting.** Handlers report done, total and stage to a pluggable
  sink; percentage and ETA are derived when read.
- **Cron scheduling.** `schedule` enqueues tasks on 5- or 6-field cron
  expressions. Schedules can be edited at runtime and are stored in memory or
  in SQL (`dbschedule`).

Everything runs in one process: the limits, dedup and schedules do not
coordinate with other processes sharing a database.

## Packages

| Package | Provides |
|---|---|
| `github.com/go-bumbu/tempo` | Runner, queue, task options, in-memory persistence, log and progress sinks |
| `github.com/go-bumbu/tempo/schedule` | Cron scheduler and an in-memory schedule store |
| `github.com/go-bumbu/tempo/dbqueue` | SQL task persistence with crash recovery (gorm) |
| `github.com/go-bumbu/tempo/dbschedule` | SQL schedule store (gorm) |
| `github.com/go-bumbu/tempo/filelog` | On-disk task log sink |

Only `dbqueue` and `dbschedule` import gorm, and only `schedule` imports
go-quartz; the core package needs neither.

## Installation

```bash
go get github.com/go-bumbu/tempo
```

## Quick start

```go
package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/go-bumbu/tempo"
)

type ScanParams struct {
	Mode string `json:"mode"` // "normal" or "full"
}

func main() {
	runner, err := tempo.NewQueueRunner(tempo.RunnerCfg{
		Parallelism: 2,                         // tasks running at once
		QueueSize:   10,                        // tasks waiting at once
		Persistence: tempo.NewMemPersistence(), // required; dbqueue for durable state
	})
	if err != nil {
		panic(err)
	}

	// Register handlers before starting the workers.
	tempo.Register(runner, "scan", func(ctx context.Context, log *slog.Logger, prog tempo.Progress, p ScanParams) error {
		log.Info("scanning", "mode", p.Mode)
		return nil
	})

	runner.StartBg()

	// Enqueue with typed params...
	if _, _, err := tempo.Enqueue(runner, "scan", ScanParams{Mode: "full"}); err != nil {
		panic(err)
	}
	// ...or by name with raw JSON, e.g. from an HTTP handler.
	if _, _, err := runner.AddRaw("scan", []byte(`{"mode":"normal"}`)); err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := runner.ShutDown(ctx); err != nil {
		panic(err)
	}
}
```

## Documentation

- [`docs/agents/`](docs/agents/README.md): the full usage guide, covering
  concepts, integration patterns, what each option guarantees, cancellation and
  shutdown, persistence and sinks, and scheduling. It is written for coding
  agents and reads just as well for people.
- Runnable examples in `example_test.go`, `example_stream_test.go`,
  `example_progress_test.go`, `filelog/example_test.go` and
  `schedule/example_test.go`, also on
  [pkg.go.dev](https://pkg.go.dev/github.com/go-bumbu/tempo).
- [AGENTS.md](AGENTS.md): guidance for agents working on tempo itself.

## License

GNU LGPL v3, see [LICENSE](LICENSE).
