<!-- todo:guide — managed by todo; this block is rewritten on save. Docs: https://github.com/andresbott/todo
This file is a todo list managed by "todo", a terminal TODO app:
https://github.com/andresbott/todo

todo watches this file and reloads it automatically when it changes on disk, so
you — human or agent — can edit it directly in any editor. Keep to this format
so todo can parse what you write:

  # Heading           Headings ("#" to "######") are categories; they nest by
                      heading level.
  - [ ] Open task     A "- [ ]" line is an open task; "- [x]" marks it done.
  - [x] Done task     Tasks must live under a category heading.
    - [ ] Subtask     Indent by two spaces to nest a subtask under a task.
    Description text  An indented, non-checkbox line is the task's description.

Notes for editors:
- Text above the first heading (this block included) is preserved on save.
- todo rewrites the file into the canonical form above on every change, so any
  other free-form markdown placed between items is not kept.
-->

Context: etna-finance and aether both carry a ~90%-identical `internal/taskrunner/`.
The "Move shared task-runner layer into tempo" section below extracts it as opt-in
subpackages so the core stays uuid-only. (This note lives above the first heading
because todo only preserves free-form text there.)

# General

- [ ] add display names to the tasks

# Move shared task-runner layer into tempo

## Backed by real duplicated code (both apps — high value, low risk)

- [x] scheduler / cron / periodic tasks — timetable, persisted + runtime-editable → `tempo/schedule` + `tempo/dbschedule` (go-quartz + gorm, out of core).
- [x] core persistence hooks + crash recovery — survive restart; orphaned "running" → "failed" on boot (`TaskStatePersistence`/`RecoverablePersistence` + `recoverTasks`, in core; tested).
- [x] DB-backed store `tempo/dbqueue` (gorm, out of core) — `RecoverablePersistence` mirror of `dbschedule`; survives restart (waiting tasks resume, orphaned "running" → "failed" via core). tested.
- [ ] per-job log files — readable + auto-cleaned → `tempo/filelog`; needs a core readable/cleanable sink iface (TaskLogSink is write-only today).
- [ ] ready-made setup — one-step constructor (runner + persistence + logs). (job-list DTO for UIs already covered by `TaskInfo` + `Runner.List()`.)

## Greenfield gaps — NOT in either app, decide if wanted

- [ ] task-level retries / backoff (re-run a failed task N times)
- [ ] progress reporting (status only today, no %/step)
- [ ] dedup / singleton enqueue

## Keep OUT of tempo (separate lib)

- [ ] HTTP resilience (retry / rate-limit / API-key rotation) + parallel-map worker pool — duplicated in the apps, but not job-runner concerns.
