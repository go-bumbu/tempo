# Integration patterns

## Choose what a task name stands for

tempo keys every rule on the task name and never reads params. So the first
design decision is what one name represents. Two shapes cover most apps:

- **One name per kind of work** (`"thumbnail"`, with the image id in the
  params). Registration is static, but every option applies to the kind as a
  whole: `WithMaxParallelism(1)` serializes all images, and `WithSingleton()`
  coalesces image B's request onto image A's waiting task, so B's run never
  happens.
- **One name per entity** (`"feed-sync:" + feedID`, with the kind of work in
  the params). Every option then applies per entity, a schedule targets the
  entity through its `TaskName`, and `Unregister` removes the entity's work. The
  cost is one registration per entity: at startup for all existing entities,
  and whenever one is created.

| You need | Name per | Use |
|---|---|---|
| Independent jobs with no per-entity rules | kind | No option, or `WithMaxParallelism(n)` to cap the kind. |
| At most one run per entity at a time, every request kept | entity | `WithMaxParallelism(1)` |
| At most one pending or running run per entity, duplicates dropped | entity | `WithSingleton()` |
| Schedules with their own cadence and params per entity | entity | Schedules whose `TaskName` is the entity's name. |
| An entity's work removed when the entity is deleted | entity | `Unregister(ctx, name)` |
| Different tasks that must not overlap on a shared resource | either | `WithExclusionGroup(resource)` |

## A task per entity, the kind in the params

```go
type FeedSyncParams struct {
	Kind string `json:"kind"` // "incremental" (default) or "full"
}

func feedTaskName(feedID string) string { return "feed-sync:" + feedID }

func registerFeed(r *tempo.QueueRunner, svc *FeedService, feedID string) {
	tempo.Register(r, feedTaskName(feedID),
		func(ctx context.Context, log *slog.Logger, prog tempo.Progress, p FeedSyncParams) error {
			switch p.Kind {
			case "", "incremental":
				return svc.SyncIncremental(ctx, feedID, prog)
			case "full":
				return svc.SyncFull(ctx, feedID, prog)
			default:
				return fmt.Errorf("unknown sync kind %q", p.Kind)
			}
		},
		tempo.WithMaxParallelism(1), // one run per feed at a time
	)
}
```

- Capture the entity's id in the closure and load its current state inside the
  handler. Capturing the whole entity would freeze its state at registration.
- **At startup, register every entity before `StartBg`.** With `dbqueue`, the
  waiting tasks recovered for those names are claimed as soon as the workers
  start, and a name without a handler fails its task.
- **When an entity is created,** register it, then create its schedules.
- **When an entity is deleted,** delete its schedules first, then call
  `Unregister`. A schedule that fires in between enqueues a task under a name
  with no handler, and that task fails.

```go
// Deleting a feed: stop new fires, then remove the handler and its tasks.
infos, err := sched.List(ctx)
if err != nil {
	return err
}
for _, s := range infos {
	if s.TaskName == feedTaskName(feedID) {
		if err := sched.Delete(ctx, s.ID); err != nil {
			return err
		}
	}
}
if err := runner.Unregister(ctx, feedTaskName(feedID)); err != nil {
	return err // a run was still stopping when ctx expired
}
```

**`WithSingleton` with several kinds:** dedup ignores params, so a `full`
request made while an `incremental` run is waiting or running coalesces onto
it, and the full sync never happens. Use `WithSingleton()` only when any run satisfies the
request ("bring this feed up to date"). When each request matters, use
`WithMaxParallelism(1)`, which queues every request and runs them one at a
time. See [task-options.md](task-options.md).

## An entity's schedules, found by task name

A schedule names the task it enqueues (`Schedule.TaskName`) and carries the
params for each fire, so one entity can have several cadences:

```go
_, err := sched.Create(ctx, schedule.Schedule{
	TaskName: feedTaskName(feedID),
	Cron:     "0 3 * * *",
	Params:   json.RawMessage(`{"kind":"incremental"}`),
	Enabled:  true,
})
```

The scheduler has no lookup by task name: list all schedules and filter on
`TaskName`, as in the delete example above. To make provisioning idempotent,
derive the schedule id from the entity: `Create` honours a supplied `ID` and
returns `schedule.ErrScheduleExists` when it is taken. See
[scheduling.md](scheduling.md).

## `Parallelism` is a resource cap

`Parallelism` is the number of workers: the most tasks running at once across
the whole runner. Size it as the budget for background work (CPU cores,
database connections, an external API's concurrency limit). It is not a
bottleneck to design around. With one name per entity, 10,000 entities do not
need 10,000 workers: their tasks wait in the queue, and `Parallelism` of them
run at a time.

- `QueueSize` caps the waiting tasks. Size it for the largest burst you
  enqueue, such as a schedule per entity all firing at 03:00. An enqueue beyond
  it returns `ErrQueueFull`, and a schedule fire that hits a full queue is
  dropped.
- `WithMaxParallelism(n)` caps one name below `Parallelism`.
- `WithExclusionGroup` keeps tasks of different names that share a resource
  from overlapping.

## Typed or raw params

| | Typed | Raw |
|---|---|---|
| Register | `tempo.Register(r, name, fn)` with `fn` taking a `T` | `r.RegisterRaw(name, fn)` with `fn` taking `[]byte` |
| Enqueue | `tempo.Enqueue(r, name, v)` JSON-encodes `v` | `r.AddRaw(name, b)` passes `b` through |

- The forms mix freely: a typed handler decodes whatever bytes arrive, whether
  from `Enqueue`, `AddRaw` or a schedule fire (fires always go through
  `AddRaw`). A schedule's `Params` must therefore be JSON for the handler's
  `T`.
- Nothing ties the `T` you enqueue to the `T` the handler decodes; only the
  name connects them. Keep one params type per task name, next to the name
  constant or name function.
- `AddRaw` does not validate. A malformed payload is accepted and fails the
  task when it runs. Validate at the edge, for example by decoding into `T` in
  the HTTP handler and then calling `Enqueue`, so bad input becomes a 400
  rather than a failed task.
- Decoding is `encoding/json`: unknown fields are ignored and missing fields
  stay zero. Payloads persisted by `dbqueue` and `dbschedule` outlive deploys,
  so evolve params types compatibly.
- tempo does not copy params slices. Do not mutate a slice after passing it to
  `AddRaw`, and do not mutate the slice a raw handler receives.

## Serving tasks to a UI or an API

- Read tasks with `r.GetTask(id)`, or `r.List()` filtered by `Name` (there is no
  index by name).
- Map `TaskInfo` to your own response type and use `Status.Str()`.
- Read a failure's reason from the log sink (`TaskLogReader.Logs`): the
  runner logs the returned error as an ERROR line.
- Read progress from the progress sink (`TaskProgressReader.Progress`), and
  compute `Percent()` and `ETA(info.StartedAt)` from it.
- After an enqueue, return the id and the `coalesced` flag. A coalesced enqueue
  returns the id of the task already in flight.

## Checklist

- [ ] Persistence chosen: `dbqueue` if waiting tasks must survive a restart.
- [ ] Every handler, per-entity ones included, registered before `StartBg`.
- [ ] `Parallelism >= 1`, and `QueueSize` sized for the largest burst.
- [ ] Handlers watch `ctx` and return `ctx.Err()` when it ends.
- [ ] A `LogSink` configured if anyone needs to know why a task failed.
- [ ] One process per `tempo_tasks` table, log directory and
      `tempo_schedules` table.
- [ ] On shutdown, the scheduler stops first, then the runner.
