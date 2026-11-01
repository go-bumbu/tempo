# Persistence and sinks

The runner has three pluggable seams. Each has a required write interface and
optional extras, detected by type assertion:

| Seam | Required | Optional | Built in |
|---|---|---|---|
| Task state (`RunnerCfg.Persistence`) | `TaskStatePersistence` | `RecoverablePersistence` (adds `List`) | `tempo.NewMemPersistence()` (not recoverable); `dbqueue.New(db)` (recoverable) |
| Logs (`RunnerCfg.LogSink`) | `TaskLogSink` | `TaskLogReader`, `TaskLogCleaner` | `tempo.NewMemTaskLogSink()`, `filelog.New(cfg)`; both implement all three |
| Progress (`RunnerCfg.ProgressSink`) | `TaskProgressSink` | `TaskProgressReader`, `TaskProgressCleaner` | `tempo.NewMemTaskProgressSink()`; implements all three |

## Task state

The queue lives in memory; persistence is a mirror of it.

- `SaveTask` (an upsert by id) is called on every enqueue and every status
  change. `RemoveTasks` is called when history is trimmed.
- **Write errors are ignored.** The runner keeps going and the store silently
  falls out of step. A task that finished while the database was unreachable
  can come back after a restart as `waiting`, and run again, or as `running`,
  and be marked `failed`. tempo does not retry writes.
- **Both methods run while the queue's lock is held.** A slow write stalls
  every enqueue, claim and status read. Calling back into the runner from them
  deadlocks.

### Recovery

Only a `RecoverablePersistence` recovers anything. `NewQueueRunner` calls
`List` and loads every task it returns:

- `waiting` tasks stay waiting and run after `StartBg`, so register their
  handlers first.
- `running` and `cancel_error` tasks were in flight when the process died. No
  worker owns them, and running them again could repeat side effects, so they
  are marked `failed` with `EndedAt` set to now. No reason is recorded: no log
  line is added, and `TaskInfo` has no error field.
- Terminal tasks become history.

Recovered tasks are all loaded, even beyond `QueueSize`. If more are waiting
than `QueueSize` allows, new enqueues get `ErrQueueFull` until enough of them
have run.

With `tempo.NewMemPersistence()` every restart starts empty: waiting tasks are
lost.

### One runner per store

`List` returns every row: nothing records which process a task belongs to. Two
runners on one store, such as two replicas of a service sharing a database,
each load and run the other's waiting tasks and mark the other's running tasks
`failed` at startup. `dbqueue` always uses the table `tempo_tasks`, so give
each runner its own database.

### `dbqueue`

- `dbqueue.New(db *gorm.DB)` runs `AutoMigrate` for `tempo_tasks` and is safe
  to call more than once. It works with any gorm dialect, and it can share a
  `*gorm.DB` with `dbschedule`.
- The status is stored as its integer code, and times are stored in UTC.
- `List` returns tasks oldest-queued first, so recovery keeps the queue's FIFO
  claim order.

### Writing your own persistence

Implement `SaveTask` as an upsert by id and `RemoveTasks` as a delete by ids.
Add `List`, returning tasks oldest-queued first, to get recovery. Keep the
writes fast and never call the runner from them. Store `Status` as given,
because recovery reads it back.

## Logs

Each handler gets a `*slog.Logger`. Its records go to `LogSink.Append`, tagged
with the task id, as `"message key=value key=value"`. Attributes are flattened
and prefixed with dotted group names. Inline `slog.Group` values and
`LogValuer`s are rendered with `Value.String()`, not resolved.

- `LogLevel` filters the handler's lines (zero is Info, so Debug lines need
  `slog.LevelDebug`).
- The runner adds its own lines whatever the level: `task started`,
  `task finished`, `task canceled`, the returned error at ERROR, and the panic
  value at ERROR.
- A task that fails because its name has no handler gets no line at all.
- `Append` runs synchronously on the worker goroutine, so it must not block.
  It is called concurrently from many tasks, so it must be safe for concurrent
  use. Its errors are ignored.
- The runner writes its own lines on a ctx that is never canceled. The
  handler's lines carry the ctx it logs with: `log.InfoContext(ctx, ...)` after
  a cancel hands the sink a canceled ctx, and a sink that honours ctx (a gorm
  one) drops the line. Log with `log.Info` or
  `log.InfoContext(context.WithoutCancel(ctx), ...)` for lines that must survive
  a cancel.

`filelog.New(filelog.Config{Dir: dir})` writes one JSON Lines file per task,
`<task id>.jsonl`, in `Dir`, creating the directory if needed. On read it
tolerates a torn final line left by a crash.

## Progress

The handler's `tempo.Progress` has `SetTotal(n)`, `Set(done)`, `Inc(delta)`
and `SetStage(msg)`. Each call sends a full `ProgressState` snapshot
(`Done`, `Total`, `Stage`, `UpdatedAt`) to the sink synchronously on the worker
goroutine. Nothing is throttled, so in a tight loop report every N items.

- Without a sink the reporter is a no-op, and `Inc` returns 0. Do not use its
  return value for control flow.
- Readers compute `Percent()` (ok is false without a `Total`) and
  `ETA(info.StartedAt)` (a linear estimate) from the snapshot. Neither is
  stored.
- The last snapshot stays after the task ends, until the task leaves history.

## Retention, and the traps in the cleaner interfaces

Logs and progress live exactly as long as their task stays in the runner's
history:

- **Trimming:** every `CleanupTimer`, the terminal tasks beyond `HistorySize`
  are trimmed. The runner then calls `RemoveTasks(ids)` on the persistence and
  on every sink that implements the cleaner interface.
- **Startup sweep:** `NewQueueRunner` calls `RetainOnly(ids)` on every sink
  that implements the cleaner interface, with the ids of the tasks it holds
  after recovery. Everything else is deleted. With `MemPersistence` the runner
  holds no tasks at startup, so every earlier line is deleted.

The traps:

1. **A sink that keeps its own history must not implement
   `TaskLogCleaner` or `TaskProgressCleaner`.** If you want logs to outlive
   tempo's task history (an audit table, a log store shared with other
   services), implement only `Append` (and `Logs`) and run your own retention.
   Otherwise the startup sweep and the trims delete it.
2. **One history serves every task name.** `HistorySize` counts terminal tasks
   of all names together, and a trim removes the oldest-queued first. A
   high-volume name pushes out the tasks of a rare one, together with their
   logs and progress. Size `HistorySize` for the total, or keep the lines you
   care about in a sink without cleaners.
3. **A log directory or table belongs to one runner.** Each runner's startup
   sweep deletes the logs of tasks it does not know, which includes every task
   of another runner pointed at the same `filelog.Dir` or table.

## Custom sink checklist

- Safe for concurrent use; `Append` and `Set` return quickly.
- `Set` is an upsert by task id: the last snapshot wins.
- Implement `TaskLogReader` or `TaskProgressReader` if a UI reads the data back.
- Implement the cleaner interfaces only if tempo should own retention.
