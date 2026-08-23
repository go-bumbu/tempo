package tempo

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
)

// TaskOption configures a registered task.
type TaskOption func(*taskOpts)

type taskOpts struct {
	maxParallelism int
	singleton      bool
}

// WithMaxParallelism caps how many instances of this task name run at once.
// 0 (the default) means no per-task limit (use the runner default).
func WithMaxParallelism(n int) TaskOption {
	return func(o *taskOpts) { o.maxParallelism = n }
}

// WithSingleton makes a task coalesce on enqueue: while an instance of this task
// name is already waiting or running, enqueuing it again enqueues nothing and
// returns the in-flight task's id. Unlike WithMaxParallelism(1) — which lets
// duplicates pile up as waiting and only serializes their execution —
// WithSingleton keeps at most one instance in the queue at all. Dedup is by task
// name, within a single process.
func WithSingleton() TaskOption {
	return func(o *taskOpts) { o.singleton = true }
}

func applyTaskOpts(opts []TaskOption) taskOpts {
	var o taskOpts
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// RegisterRaw registers a handler that receives the raw parameter bytes.
// Use it for tasks whose name/payload are known only at runtime, or that decode
// the payload themselves. Overwrites any handler already registered for name.
// tempo does not copy the params slice: callers must not mutate a slice passed
// to AddRaw after the call, and raw handlers must not mutate the params slice
// they receive.
func (r *QueueRunner) RegisterRaw(name string, fn func(ctx context.Context, log *slog.Logger, params []byte) error, opts ...TaskOption) {
	o := applyTaskOpts(opts)
	r.registry.add(name, registered{run: fn, maxParallelism: o.maxParallelism, singleton: o.singleton})
}

// enqueue routes name onto the queue, honouring a singleton registration: a task
// registered WithSingleton is added via the queue's AddUnique so a duplicate
// coalesces onto the instance already waiting or running. Unknown or
// non-singleton names use the unconditional Add.
func (r *QueueRunner) enqueue(name string, params []byte) (uuid.UUID, error) {
	if entry, ok := r.registry.lookup(name); ok && entry.singleton {
		return r.queue.AddUnique(name, params)
	}
	return r.queue.Add(name, params)
}

// AddRaw enqueues a task by name with a raw parameter payload (may be nil).
// tempo does not copy the params slice: callers must not mutate a slice passed
// to AddRaw after the call, and raw handlers must not mutate the params slice
// they receive.
func (r *QueueRunner) AddRaw(name string, params []byte) (uuid.UUID, error) {
	return r.enqueue(name, params)
}

// Register registers a typed task handler. Parameters are JSON-decoded into T
// before fn runs; an empty payload yields a zero-value T. T is inferred from fn.
func Register[T any](r *QueueRunner, name string, fn func(ctx context.Context, log *slog.Logger, params T) error, opts ...TaskOption) {
	o := applyTaskOpts(opts)
	r.registry.add(name, registered{
		run: func(ctx context.Context, log *slog.Logger, raw []byte) error {
			var p T
			if len(raw) > 0 {
				if err := json.Unmarshal(raw, &p); err != nil {
					return fmt.Errorf("tempo: decode params for task %q: %w", name, err)
				}
			}
			return fn(ctx, log, p)
		},
		maxParallelism: o.maxParallelism,
		singleton:      o.singleton,
	})
}

// Enqueue enqueues a task by name with typed parameters, JSON-encoded. T is
// inferred from params.
func Enqueue[T any](r *QueueRunner, name string, params T) (uuid.UUID, error) {
	raw, err := json.Marshal(params)
	if err != nil {
		return uuid.Nil, fmt.Errorf("tempo: encode params for task %q: %w", name, err)
	}
	return r.enqueue(name, raw)
}
