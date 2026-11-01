# AGENTS.md

Guidance for coding agents working with this repository.

tempo (`github.com/go-bumbu/tempo`) is a Go library: an in-process background
job runner with a bounded worker pool, typed and raw task parameters, per-task
concurrency options, pluggable persistence, log and progress sinks, and a cron
scheduler. It builds no binary and runs no service.

## Using tempo from another project

Read [`docs/agents/`](docs/agents/README.md) before you write integration code
or size a migration onto tempo. Its README lists the ten facts that prevent
most integration mistakes, then links to:

- [concepts.md](docs/agents/concepts.md): the parts, a task's lifecycle, the
  API, configuration and zero values.
- [integration-patterns.md](docs/agents/integration-patterns.md): task per
  entity or per kind, the kind in the params, an entity's schedules,
  `Parallelism` as a resource cap, typed or raw params.
- [task-options.md](docs/agents/task-options.md): what `WithMaxParallelism`,
  `WithSingleton` and `WithExclusionGroup` guarantee, and what they don't.
- [cancel-and-shutdown.md](docs/agents/cancel-and-shutdown.md): how a run
  ends, `Cancel`, `Unregister`, `ShutDown`, zero-value configuration.
- [persistence-and-sinks.md](docs/agents/persistence-and-sinks.md): task
  state and crash recovery, log and progress sinks, the traps in the cleaner
  interfaces.
- [scheduling.md](docs/agents/scheduling.md): the cron scheduler and its
  limits.

The root [README.md](README.md) is a capability summary for people discovering
the project, not a usage reference.

## Working on tempo

### Layout

| Path | Contents |
|---|---|
| `runner.go` | `QueueRunner`: workers, `Cancel`, `Unregister`, `ShutDown`, history cleanup. |
| `queue.go` | `TaskQueue`, task statuses, `TaskInfo`, the persistence interfaces, `MemPersistence`, recovery. |
| `params.go` | `Register`/`RegisterRaw`, `Enqueue`/`AddRaw`, task options. |
| `registry.go`, `limiter.go` | Handlers by name; per-name and exclusion-group slots. |
| `logs.go`, `progress.go` | Log and progress sink interfaces, the per-task reporters, the in-memory sinks. |
| `schedule/` | Cron `Scheduler` on go-quartz, the `Store` interface, `MemStore`, cron normalization. |
| `dbqueue/`, `dbschedule/` | gorm stores for tasks (`tempo_tasks`) and schedules (`tempo_schedules`). |
| `filelog/` | JSON Lines log sink, one file per task. |
| `docs/agents/` | The usage docs for agents. |

### Commands

- `make test`: run the tests.
- `make test-race`: run the tests with the race detector, `-count 10`. CI runs this.
- `make lint`: golangci-lint with `.golangci.yaml`.
- `make coverage`: fails when a package is below 80%. CI runs this.
- `make verify`: lint, race tests, license check, benchmarks and coverage.

### Conventions

- **Keep dependencies out of the core.** Package `tempo` imports only the
  standard library and `google/uuid`. gorm stays in `dbqueue` and
  `dbschedule`, go-quartz in `schedule`. That is why those live in their own
  packages.
- **Persisted data is a contract.** `dbqueue` stores `TaskStatus` as its
  integer code, so never renumber or reorder the status constants. Table and
  column changes must stay compatible with `AutoMigrate` on existing
  databases.
- **Mind the lock order** documented on `QueueRunner` in `runner.go`: the queue
  lock first, then `runMu` or the registry lock, never the reverse.
  Persistence writes happen under the queue lock.
- **Tests:** timing-dependent tests use `testing/synctest`. Example functions
  with an `// Output:` comment are run by `go test`, so keep their output
  deterministic.
- **Docs follow behavior.** A change in behavior updates the matching page in
  `docs/agents/`, and the capability list in `README.md` when a capability is
  added or removed. The pages describe current behavior, known issues
  included; when you fix one, update the page that warns about it.
- **`TODO.md`** is managed by the `todo` terminal app. Keep its format; the
  comment at the top of the file describes it.
- **Commits and PR titles** use Conventional Commits, for example
  `feat(runner): ...` or `fix(filelog): ...`, with `!` for a breaking change.
