# Concepts

## The parts

- **`QueueRunner`**: the whole runtime. An in-memory queue of tasks, a registry
  of handlers keyed by task name, and `Parallelism` worker goroutines. Built
  with `tempo.NewQueueRunner(tempo.RunnerCfg{...})`.
- **Task name**: the key a handler is registered under (`"scan"`,
  `"feed-sync:42"`). Options, dedup, exclusion groups, schedules and
  `Unregister` all work on names.
- **Task**: one enqueued run of a name. It has a `uuid.UUID` id, its params
  bytes, a status and timestamps (`tempo.TaskInfo`). Enqueuing a name twice
  makes two tasks.
- **Handler**: the function a worker calls to run a task.
- **Persistence** (required): mirrors task state, either in memory
  (`tempo.NewMemPersistence()`) or in SQL (`dbqueue`).
- **Log and progress sinks** (optional): receive what handlers log and report.
- **`schedule.Scheduler`**: a separate package that enqueues tasks when cron
  expressions fire. It only enqueues; the runner runs them.

All of it lives in one process. Nothing coordinates with another process, even
when both use the same database.

## Handlers

Both registration forms take a handler of this shape:

```go
func(ctx context.Context, log *slog.Logger, prog tempo.Progress, params T) error
```

- `tempo.Register(runner, name, fn, opts...)` JSON-decodes the payload into
  `T`, inferred from `fn`. An empty payload gives the zero `T`; a payload that
  does not decode fails the task.
- `runner.RegisterRaw(name, fn, opts...)` hands `fn` the raw `[]byte`.

What the handler receives:

- `ctx` is canceled by `Cancel`, `Unregister` and `ShutDown`. It derives from
  the runner, not from whoever enqueued the task: `AddRaw` and `Enqueue` take no
  context, so request-scoped values (auth, trace ids, deadlines) never reach
  the task. Put what the task needs in its params.
- `log` writes to `RunnerCfg.LogSink`, tagged with the task id. It discards
  everything when no sink is configured.
- `prog` reports progress to `RunnerCfg.ProgressSink`. It is a no-op when no
  sink is configured, so calling it is always safe.
- The return value decides the final status (see below).

Registering a name again replaces its handler and options. A task already
running keeps the old handler; waiting tasks get the new one when claimed.

## Lifecycle

```text
enqueue --> waiting --claim--> running --return--> complete | failed | panicked | canceled
               |                  |
               | Cancel /         | Cancel / Unregister gives up waiting
               | Unregister       v
               v            cancel_error --return--> complete | failed | panicked | canceled
            canceled
```

| Status | `Str()` | Terminal | Meaning |
|---|---|---|---|
| `TaskStatusWaiting` | `waiting` | no | Queued, not claimed yet. |
| `TaskStatusRunning` | `running` | no | A worker is running its handler. |
| `TaskStatusComplete` | `complete` | yes | The handler returned `nil`. |
| `TaskStatusFailed` | `failed` | yes | The handler returned an error that does not wrap `context.Canceled`; or no handler was registered for the name; or the params did not decode; or the task was in flight when the process died (set by recovery); or the handler ended its goroutine (`runtime.Goexit`). |
| `TaskStatusPanicked` | `panicked` | yes | The handler panicked. The panic is recovered and its value logged at ERROR, without a stack trace. |
| `TaskStatusCanceled` | `canceled` | yes | Canceled while waiting, or the handler returned an error wrapping `context.Canceled`. |
| `TaskStatusCancelError` | `cancel_error` | no | A `Cancel` or `Unregister` gave up waiting for the run to stop. The run is still going and ends with the status its return value gives. |

`TaskStatus` has a `Str()` method, not `String()`: `%v` prints the number.
`tempo.TaskTerminalStatus` lists the four terminal statuses.

`canceled` means "the handler returned an error wrapping `context.Canceled`",
not "someone called `Cancel`". A handler that bubbles up `context.Canceled`
from an inner call ends `canceled`; one that returns its own timeout's
`context.DeadlineExceeded` ends `failed`.

## Wiring order

1. `tempo.NewQueueRunner(cfg)`. With a recoverable persistence such as
   `dbqueue` this loads the persisted tasks. It also sweeps the sinks (see
   [persistence-and-sinks.md](persistence-and-sinks.md)).
2. Register every handler.
3. `StartBg()`, exactly once (a second call panics). It starts the workers and
   the history cleanup. Recovered waiting tasks are claimed right away, which
   is why step 2 comes first.
4. Enqueue, query and cancel.
5. `ShutDown(ctx)`, after `StartBg` has been called: on a runner that was never
   started it blocks forever, ignoring its ctx. A runner cannot be restarted;
   build a new one.

## API at a glance

| Call | Returns | Notes |
|---|---|---|
| `tempo.NewQueueRunner(cfg RunnerCfg)` | `(*QueueRunner, error)` | Fails only when `Persistence` is nil. |
| `tempo.Register(r, name, fn, opts...)` | | Typed handler; `T` inferred from `fn`. |
| `r.RegisterRaw(name, fn, opts...)` | | Raw handler. |
| `r.StartBg()` | | Call exactly once. |
| `tempo.Enqueue(r, name, params)` | `(uuid.UUID, bool, error)` | JSON-encodes `params`. The bool reports a coalesced singleton enqueue. |
| `r.AddRaw(name string, params []byte)` | `(uuid.UUID, bool, error)` | `params` may be nil. Not copied, not validated. |
| `r.GetTask(id uuid.UUID)` | `(TaskInfo, error)` | `ErrTaskNotFound` once the task is trimmed from history. |
| `r.List()` | `[]TaskInfo` | Every task the runner holds, newest-queued first. |
| `r.Cancel(ctx, id)` | `error` | See [cancel-and-shutdown.md](cancel-and-shutdown.md). |
| `r.Unregister(ctx, name)` | `error` | Removes the handler and cancels the name's tasks. |
| `r.ShutDown(ctx)` | `error` | `ErrUnsafeStop` when ctx expires first. Idempotent. |
| `r.Wait()` | | Blocks until `ShutDown` has finished. |

`TaskQueue`, `NewTaskQueue` and `TaskQueueCfg` are exported, but they are the
runner's plumbing. Do not drive them directly.

Sentinel errors: `ErrQueueFull`, `ErrTaskNotFound`, `ErrUnsafeStop`. Everything
else is a plain formatted error: `task not cancelable: status <status>`,
`cancel timeout: <ctx error>`, `unregister timeout: <ctx error>`, and
`tempo: encode params for task "<name>": ...` from `Enqueue`. A decode failure
is not returned to anyone; it fails the task and is logged.

## TaskInfo

```go
type TaskInfo struct {
	ID        uuid.UUID
	Name      string
	Status    TaskStatus
	QueuedAt  time.Time
	StartedAt time.Time
	EndedAt   time.Time
	Params    []byte
}
```

- There is no error or reason field. Why a task failed is only in the log sink,
  as an ERROR line with the error text.
- There are no JSON tags. Encoding it directly gives `Status` as an integer and
  `Params` as base64. Map it to your own type for an API.

## Configuration

| `RunnerCfg` field | Meaning | Zero value |
|---|---|---|
| `Parallelism` | Worker goroutines: the most tasks running at once, all names together. | Required; below 1 is an error. |
| `QueueSize` | The most tasks waiting at once. An enqueue beyond it returns `ErrQueueFull`. | Required; below 1 is an error. |
| `HistorySize` | Terminal tasks kept, all names together. | 10 |
| `CleanupTimer` | How often history is trimmed. | 5 minutes |
| `Persistence` | Task state mirror. | Required; nil is an error. |
| `LogSink` | Receives handler logs and the runner's lifecycle lines. | Handler logs are discarded. |
| `LogLevel` | Minimum level of handler lines sent to the sink. | `slog.LevelInfo` |
| `ProgressSink` | Receives progress updates. | Progress is discarded. |

## Queue behavior

- **Claim order.** A worker scans the waiting tasks oldest-first and takes the
  first one its limits allow (`WithMaxParallelism`, exclusion group). A blocked
  task does not hold up the others, but nothing guarantees fairness: a blocked
  task can be overtaken again and again. There are no priorities.
- **`QueueSize` counts waiting tasks only.** Running and finished tasks do not
  count against it.
- **History.** A finished task stays visible to `GetTask` and `List` until it
  is trimmed. Every `CleanupTimer` the runner trims the terminal tasks beyond
  `HistorySize`, oldest-queued first and across all names, and removes their
  persisted state, logs and progress. Between two trims the history can exceed
  `HistorySize`.
- **No retries.** A failed task stays failed; enqueue it again to retry.
