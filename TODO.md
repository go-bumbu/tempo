<!-- todo:guide — managed by todo; this block is rewritten on save. Docs: https://github.com/candy-tools/todo
This file is a todo list managed by "todo", a terminal TODO app:
https://github.com/candy-tools/todo

todo watches this file and reloads it automatically when it changes on disk, so
you — human or agent — can edit it directly in any editor. Keep to this format
so todo can parse what you write:

  # Heading           Headings ("#" to "######") are categories; they nest by
                      heading level.
  - [ ] Open task     A "- [ ]" line is an open task; "- [x]" marks it done.
  - [/] In progress   "- [/]" flags a task in progress, "- [>]" defers it.
  - [x] Done task     Tasks must live under a category heading.
    - [ ] Subtask     Indent by two spaces to nest a subtask under a task.
    Description text  An indented, non-checkbox line is the task's description.

Notes for editors:
- Text above the first heading (this block included) is preserved on save.
- todo rewrites the file into the canonical form above on every change, so any
  other free-form markdown placed between items is not kept.

Prefer the CLI over editing by hand: it writes this file in the exact format
above. `todo add`, `todo done|progress|defer|reopen`, `todo edit`, `todo rm`
and `todo list --json` cover the common actions — run `todo --help`.
-->

Context: etna-finance and aether both carry a ~90%-identical `internal/taskrunner/`.
The "Move shared task-runner layer into tempo" section below extracts it as opt-in
subpackages so the core stays uuid-only. (This note lives above the first heading
because todo only preserves free-form text there.)

# General

- [>] add display names to the tasks
- [>] task-level retries / backoff
  Re-run a failed task N times.
- [ ] give Cancel's errors sentinels
  "cancel timeout" wraps ctx.Err(), so a caller whose own ctx was canceled
  gets errors.Is(err, context.Canceled) and can read it as the task canceled.
  Add ErrCancelTimeout and ErrNotCancelable; decide whether a second Cancel
  of a canceled task returns nil. Unregister's "unregister timeout" wraps
  ctx.Err() the same way.
- [ ] keep waiting tasks waiting on ShutDown
  nextTask looks for a waiting task before it checks ctx, so after ShutDown
  the workers claim every waiting task and run it on a canceled ctx, and it
  ends canceled. With dbqueue, a graceful restart leaves nothing to resume.

# From the ifmrip jobs migration

- [x] fix the race on a canceled task's final status
  Cancel sets Canceled after a running task returns, while the worker sets
  the task's own result at the same moment: the last write wins. A task
  that returns nil on ctx.Done (the README's example) ends Complete or
  Canceled by timing. Let the worker write the one final status.
- [x] add Unregister to the runner
  A task registered per entity (one per ifmrip job) stays registered after
  the entity is deleted, until restart. Decide what happens to its waiting
  tasks.
- [>] allow a time zone per schedule
  Cfg.Location is the one zone for every rule; a server in another zone
  than its jobs sets it once. Add an optional Schedule.TimeZone, stored by
  dbschedule (quartz already takes a zone per trigger), only if one store
  must hold rules in different zones.
- [x] refuse or default a zero Parallelism and QueueSize
  Parallelism 0 starts no worker, so nothing runs; QueueSize 0 makes every
  enqueue fail with ErrQueueFull.
- [x] log a canceled task's last lines on a live context
  "task canceled", the error and a panic are appended on the task's own,
  already canceled ctx, so a sink that honours ctx (a gorm one) drops
  them. Use context.WithoutCancel.
- [ ] document how to integrate tempo, for agents
  An agent misread the API while sizing ifmrip's migration: it missed that
  one task per entity gives one run per entity, and took the worker pool
  for a drawback, not a resource cap. Add an AGENTS.md covering:
  - [ ] integration patterns
    A task per entity with the kind in the params, an entity's schedules
    by task name, Parallelism as a resource cap, typed vs raw params.
  - [ ] what each option guarantees
    Singleton coalesces, an exclusion group queues, max parallelism caps
    one name; none spans processes.
  - [ ] traps of the pluggable interfaces
    RetainOnly at startup deletes the logs of tasks tempo doesn't know, so
    a sink that keeps its own history must not implement the cleaners;
    RemoveTasks trims one history shared by all names; recovery runs
    waiting tasks and fails running ones with no reason.
  - [ ] cancel and shutdown
    Return ctx.Err() to record Canceled; what ShutDown does to running and
    waiting tasks; the zero values above.
  - [ ] the scheduler's limits
    One process per store, a server-wide zone, missed fires not replayed,
    a coalesced fire is skipped.
  - [ ] one Time zone configuraion for all jobs
