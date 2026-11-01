# tempo for coding agents

How to integrate `github.com/go-bumbu/tempo` into an application correctly.
These pages describe what the code does today, known issues included. When a
page and the code disagree, the code wins: fix the page.

## Read in this order

| Page | Read it when |
|---|---|
| [concepts.md](concepts.md) | Always, first: the parts, a task's lifecycle, the API, configuration and its zero values. |
| [integration-patterns.md](integration-patterns.md) | Mapping your domain onto task names, params and schedules; sizing the runner. |
| [task-options.md](task-options.md) | Using `WithMaxParallelism`, `WithSingleton` or `WithExclusionGroup`. |
| [cancel-and-shutdown.md](cancel-and-shutdown.md) | Writing handlers, canceling tasks, removing handlers, stopping the process. |
| [persistence-and-sinks.md](persistence-and-sinks.md) | Durable state and crash recovery, task logs and progress, writing your own store or sink. |
| [scheduling.md](scheduling.md) | Running tasks on cron expressions. |

## Ten facts that prevent most mistakes

1. **The task name is the unit of identity.** Options, singleton dedup,
   exclusion groups, schedules (`Schedule.TaskName`) and `Unregister` all key
   on the name; tempo never looks inside params. One name per entity gives
   one-run-per-entity rules; one name per kind applies them to every entity at
   once. See [integration-patterns.md](integration-patterns.md).
2. **`Parallelism` is the resource cap**: the most tasks running at once across
   the whole runner. Extra tasks wait in the queue; that is the design, not a
   limitation to engineer around. `QueueSize` caps how many may wait.
3. **Zero values are not defaulted.** `Parallelism: 0` starts no worker, so
   nothing ever runs; `QueueSize: 0` rejects every enqueue with `ErrQueueFull`.
4. **Register every handler before `StartBg`.** Tasks recovered from a durable
   store are claimed as soon as the workers start, and a task whose name has no
   handler at that moment fails.
5. **A name without a handler is accepted** by `AddRaw` and `Enqueue`, and the
   task then fails with no log line saying why. A typo in a task name is a
   failed task, not an error.
6. **A task's error is not stored on the task.** `TaskInfo` has no error field;
   the error text only reaches the `LogSink`, as an ERROR line. No sink, no
   failure reasons.
7. **Return `ctx.Err()` when the task stops because its context ended.** An
   error wrapping `context.Canceled` records `canceled`; returning `nil`
   records `complete`, even for a task that was asked to stop.
8. **`ShutDown` runs the waiting tasks on an already-canceled context** before
   it returns (known issue). A cancel-aware handler ends them `canceled`, so
   with `dbqueue` nothing is left to resume after a restart.
9. **Nothing spans processes.** Limits, dedup, exclusion and the scheduler hold
   within one process. Run one runner per `tempo_tasks` table and log
   directory, and one scheduler per `tempo_schedules` table.
10. **Sinks that implement the cleaner interfaces get swept.** At startup
    `RetainOnly` deletes the logs and progress of every task the runner does not
    know, and trimming the history, which all task names share, deletes the
    rest over time.
