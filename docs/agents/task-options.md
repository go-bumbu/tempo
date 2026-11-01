# Task options

Options are passed when registering a name and apply to every task of that
name:

```go
tempo.Register(r, "scan", scan, tempo.WithMaxParallelism(1), tempo.WithExclusionGroup("files"))
r.RegisterRaw("reindex", reindex, tempo.WithSingleton(), tempo.WithExclusionGroup("files"))
```

Registering the name again replaces its options along with its handler. The
limits are checked when a worker claims a task, and the singleton check when a
task is enqueued.

## `WithMaxParallelism(n)`

**Guarantees:** at most `n` tasks of this name run at once, within this
runner.

**Does not:**

- Limit enqueues. Extra tasks wait, and pile up until `QueueSize` is reached.
- Drop or merge anything. Every enqueued task eventually runs.
- Hold across processes.

`n <= 0` means no per-name cap; the runner's `Parallelism` still applies.

## `WithSingleton()`

**Guarantees:** while a task of this name is `waiting`, `running` or
`cancel_error`, enqueuing the name again adds nothing and returns
`(id of the task in flight, true, nil)`. Once that task reaches a terminal
status, the next enqueue starts a fresh task and returns `(new id, false, nil)`.
At most one task of the name is in flight at a time, so at most one runs.

**Does not:**

- Look at params. Dedup is by name only, and the coalesced request's params
  are discarded. With one name per kind and the entity in the params, entity
  B's request coalesces onto entity A's task and B never runs.
- Re-enqueue later. A coalesced request is gone, not deferred.
- Return an error. `ErrQueueFull` and other errors are only for a real insert;
  check the `coalesced` bool to tell a duplicate from a fresh task.
- Hold across processes.

It also applies to schedule fires (a coalesced fire is logged as a skip and
dropped) and to `Scheduler.Trigger`, which returns the in-flight task's id
without saying it coalesced.

## `WithExclusionGroup(name)`

**Guarantees:** among all names registered with the same group, at most one
task runs at a time. A waiting group member does not block the queue; workers
run other eligible tasks and claim the member once the group frees.

**Does not:**

- Order the group's members. It is mutual exclusion, not a pipeline.
- Put a name in several groups. A registration has one group, and the last
  `WithExclusionGroup` passed wins.
- Allow more than one at a time; group capacity is fixed at 1.
- Hold across processes.

An empty group name means no group. Group names and task names are separate
namespaces.

## Side by side

| | Scope | Acts at | Extra requests | Typical use |
|---|---|---|---|---|
| `WithMaxParallelism(n)` | One name | Claim time | Wait their turn | Cap a heavy task, or one run per entity while keeping every request. |
| `WithSingleton()` | One name | Enqueue time | Dropped (coalesced) | "Refresh now" buttons; overlapping schedule fires. |
| `WithExclusionGroup(g)` | Several names | Claim time | Wait their turn | Tasks of different kinds that share a resource. |

The options compose. For example, give each entity its own name with
`WithSingleton()` and put all of them in a `WithExclusionGroup("disk")`: at most
one pending run per entity, and one run at a time on the disk.

## What no option provides

- Coordination across processes or across runners.
- Priorities. Claim order is oldest-first among the tasks whose limits allow
  them to run.
- Rate limiting over time ("at most N per minute").
- Retries. A failed task stays failed.
