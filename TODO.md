- [] add display names to the tasks

## Move shared task-runner layer into tempo
etna-finance and aether both carry a ~90%-identical `internal/taskrunner/`. Extract as opt-in subpackages so the core stays uuid-only.

Backed by real duplicated code (both apps — high value, low risk):
- [x] scheduler / cron / periodic tasks — timetable, persisted + runtime-editable → `tempo/schedule` + `tempo/dbschedule` (go-quartz + gorm, out of core).
- [] DB persistence + crash recovery — survive restart; orphaned "running" → "failed" on boot → `tempo/dbqueue` (gorm, out of core).
- [] per-job log files — readable + auto-cleaned → `tempo/filelog`; needs a core readable/cleanable sink iface (TaskLogSink is write-only today).
- [] ready-made setup — one-step constructor (runner + persistence + logs) + job-list DTO for UIs.

Greenfield gaps — NOT in either app, decide if wanted:
- [] task-level retries / backoff (re-run a failed task N times)
- [] progress reporting (status only today, no %/step)
- [] dedup / singleton enqueue

Keep OUT of tempo (separate lib):
- [] HTTP resilience (retry / rate-limit / API-key rotation) + parallel-map worker pool — duplicated in the apps, but not job-runner concerns.
