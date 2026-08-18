package tempo

import (
	"context"

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
