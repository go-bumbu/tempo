# Cancel and shutdown

## How a run ends

The handler's return value alone decides the final status, whoever stopped it:

| The handler | Status |
|---|---|
| Returns `nil` | `complete`, even if it was asked to stop |
| Returns an error wrapping `context.Canceled` (such as `ctx.Err()` after a cancel) | `canceled` |
| Returns any other error, `context.DeadlineExceeded` included | `failed` |
| Panics | `panicked` |
| Ends its goroutine without returning (`runtime.Goexit`) | `failed` |

`Cancel`, `Unregister` and `ShutDown` stop a running task only by canceling
its ctx. A handler that never checks ctx runs to the end.

```go
func syncItems(ctx context.Context, log *slog.Logger, prog tempo.Progress, p SyncParams) error {
	items, err := loadItems(ctx, p)
	if err != nil {
		return err // wraps context.Canceled if ctx ended mid-load: canceled
	}
	prog.SetTotal(int64(len(items)))
	for _, it := range items {
		if err := ctx.Err(); err != nil {
			log.Info("stopping early", "reason", err)
			return err // Cancel, Unregister or ShutDown: canceled
		}
		if err := process(ctx, it); err != nil {
			return fmt.Errorf("item %s: %w", it.ID, err)
		}
		prog.Inc(1)
	}
	return nil
}
```

Do cleanup before returning: once the handler returns, the status is recorded
and `Cancel` returns to its caller.

## `Cancel(ctx, id)`

- **A waiting task** is marked `canceled` at once and never runs. `Cancel`
  returns nil.
- **A running task** (or one in `cancel_error`) has its ctx canceled.
  `Cancel` waits until the handler returns, so its final status is already
  recorded, and returns nil.
- **When `Cancel`'s own ctx ends first**, it returns
  `cancel timeout: <ctx error>` and the task shows `cancel_error` until it
  stops. It then gets the status its return value gives.
- **Unknown id:** `ErrTaskNotFound`.
- **A finished task,** including one already canceled:
  `task not cancelable: status <status>`.

**Trap:** the timeout error wraps the caller's ctx error. When the caller's ctx
was canceled, for example because the HTTP request went away,
`errors.Is(err, context.Canceled)` is true, yet the task was not necessarily
canceled. Read the task's status with `GetTask` instead of inferring it from
the error. There are no sentinel errors for "timeout" or "not cancelable" yet.

## `Unregister(ctx, name)`

Removes the name's handler and cancels the name's tasks the way `Cancel` does:
waiting ones are marked `canceled`, and running ones have their ctx canceled.
It returns once the running ones have stopped. If its ctx ends first, it
returns `unregister timeout: <ctx error>` and the runs still going show
`cancel_error`.

- A name with no handler is not an error.
- Until the name is registered again, an enqueue under it is accepted and the
  task then fails. Delete the name's schedules before unregistering.
- `Unregister` waits for the name's running tasks, so one of those tasks must
  not call it and wait: it would wait for itself until ctx expires. From inside
  such a task, call it in a new goroutine and return.

## `ShutDown(ctx)`

What happens when it is called:

1. The runner's context is canceled, so every running task's ctx is canceled.
2. **Waiting tasks are claimed and run on that already-canceled context** before
   the workers exit (known issue, tracked in `TODO.md` as "keep waiting tasks
   waiting on ShutDown"). A cancel-aware handler returns at once and the task
   ends `canceled`; a handler that ignores ctx runs to completion during the
   shutdown. Nothing stays `waiting`, so with `dbqueue` a graceful restart
   finds no waiting work to resume. If queued work must survive a deploy,
   record it in your own store and enqueue it again at startup.
3. `ShutDown` waits for the workers to exit. If its ctx ends first, it returns
   `ErrUnsafeStop`. The tasks are not killed and keep running in the
   background until the process exits; with `dbqueue`, the next start recovers
   them as `failed`.

Rules:

- **Call `StartBg` before `ShutDown`.** On a runner that was never started,
  `ShutDown` blocks forever and ignores its ctx.
- `ShutDown` is idempotent: a second call returns nil.
- `Wait()` blocks until `ShutDown` has finished.
- An enqueue after `ShutDown` is accepted, and the task stays `waiting`: no
  worker is left to run it. With `dbqueue`, the next process runs it.
- A runner cannot be restarted. A second `StartBg` panics; build a new runner.

A typical process shutdown, with the scheduler stopped first so no fire
enqueues into a stopping runner:

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()
<-ctx.Done()

shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
if err := sched.ShutDown(shutdownCtx); err != nil {
	slog.Error("scheduler shutdown", "error", err)
}
if err := runner.ShutDown(shutdownCtx); err != nil {
	slog.Error("runner shutdown: tasks still running", "error", err) // tempo.ErrUnsafeStop
}
```

## Zero-value configuration

Neither value is validated (tracked in `TODO.md`):

- `Parallelism: 0` starts no worker. Every task stays `waiting` forever.
- `QueueSize: 0` makes every enqueue return `ErrQueueFull`.

The other zero values have defaults: `HistorySize` 10, `CleanupTimer` 5
minutes, `LogLevel` `slog.LevelInfo`.
