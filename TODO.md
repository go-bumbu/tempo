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
- [x] Don't fail a whole log read when the last line is truncated or corrupt
  In plain terms: when a job writes its log to a file and the program crashes or is killed mid-write, the file's last line can be left half-written. Reading that job's log then fails completely — you get an error and see none of the log, even though every line but the last is perfectly fine. To anyone looking at the log afterwards (say, in a web UI), a job that actually ran fine but was interrupted looks like it produced no log at all. The fix is to ignore a broken last line and still return all the good ones.
  Technical: filelog/store.go Logs() reads the whole <id>.jsonl file, splits on "\n", and json.Unmarshal's each non-empty segment; the first unmarshal error aborts the entire call (returns nil, err). Append writes "<json>\n" in a single O_APPEND write, but a crash/kill (or ENOSPC) between appends can still leave a partial trailing line with no newline, which fails to parse — so Logs errors and any consumer that serves the log over HTTP returns 5xx for the whole log instead of the intact prefix. Preferred fix: tolerate a malformed FINAL segment only — parse every complete line and, if the last split segment fails to unmarshal, drop it and return the successfully-parsed prefix with no error; a parse failure on a non-final line is genuine mid-file corruption and may still error (or be surfaced separately). Alternatives: skip any unparseable line and continue (optionally log a warning), or make per-line writes crash-atomic. Test: write one valid JSON line followed by a truncated fragment (no trailing newline) and assert Logs returns the single valid entry and no error.

# Move shared task-runner layer into tempo

## Backed by real duplicated code (both apps — high value, low risk)

- [x] scheduler / cron / periodic tasks
  Timetable, persisted + runtime-editable → `tempo/schedule` + `tempo/dbschedule` (go-quartz + gorm, out of core).
- [x] core persistence hooks + crash recovery
  Survive restart; orphaned "running" → "failed" on boot (`TaskStatePersistence`/`RecoverablePersistence` + `recoverTasks`, in core; tested).
- [x] DB-backed store `tempo/dbqueue`
  Gorm, out of core. `RecoverablePersistence` mirror of `dbschedule`; survives restart (waiting tasks resume, orphaned "running" → "failed" via core). Tested.
- [x] per-job log files
  Readable + auto-cleaned → `tempo/filelog`; needs a core readable/cleanable sink iface (TaskLogSink is write-only today).

## Greenfield gaps — NOT in either app, decide if wanted

- [ ] task-level retries / backoff
  Re-run a failed task N times.
- [ ] progress reporting
  Status only today, no %/step.
- [x] dedup / singleton enqueue
