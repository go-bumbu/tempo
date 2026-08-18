package tempo

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

// TaskOption configures a registered task.
type TaskOption func(*taskOpts)

type taskOpts struct {
	maxParallelism int
}

// WithMaxParallelism caps how many instances of this task name run at once.
// 0 (the default) means no per-task limit (use the runner default).
func WithMaxParallelism(n int) TaskOption {
	return func(o *taskOpts) { o.maxParallelism = n }
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
func (r *QueueRunner) RegisterRaw(name string, fn func(ctx context.Context, params []byte) error, opts ...TaskOption) {
	o := applyTaskOpts(opts)
	r.registry.add(name, registered{run: fn, maxParallelism: o.maxParallelism})
}

// AddRaw enqueues a task by name with a raw parameter payload (may be nil).
func (r *QueueRunner) AddRaw(name string, params []byte) (uuid.UUID, error) {
	return r.queue.Add(name, params)
}

// Register registers a typed task handler. Parameters are JSON-decoded into T
// before fn runs; an empty payload yields a zero-value T. T is inferred from fn.
func Register[T any](r *QueueRunner, name string, fn func(ctx context.Context, params T) error, opts ...TaskOption) {
	o := applyTaskOpts(opts)
	r.registry.add(name, registered{
		run: func(ctx context.Context, raw []byte) error {
			var p T
			if len(raw) > 0 {
				if err := json.Unmarshal(raw, &p); err != nil {
					return fmt.Errorf("tempo: decode params for task %q: %w", name, err)
				}
			}
			return fn(ctx, p)
		},
		maxParallelism: o.maxParallelism,
	})
}

// Enqueue enqueues a task by name with typed parameters, JSON-encoded. T is
// inferred from params.
func Enqueue[T any](r *QueueRunner, name string, params T) (uuid.UUID, error) {
	raw, err := json.Marshal(params)
	if err != nil {
		return uuid.Nil, fmt.Errorf("tempo: encode params for task %q: %w", name, err)
	}
	return r.queue.Add(name, raw)
}
