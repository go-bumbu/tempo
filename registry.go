package tempo

import (
	"context"
	"sync"
)

// TaskDef defines how to run a task and optional per-task behavior.
// It is the param-less task descriptor consumed by GroupRunner.
type TaskDef struct {
	// Name identifies the task (used for registration and enqueueing).
	Name string
	// Run is the function to execute for this task.
	Run func(ctx context.Context) error
	// MaxParallelism is the max number of this task name that may run at once.
	// 0 means no per-task limit (use runner default).
	MaxParallelism int
}

// registered is the internal, erased form of a task handler stored in the registry.
type registered struct {
	run            func(ctx context.Context, params []byte) error
	maxParallelism int
}

// taskRegistry is the internal in-memory registry; only the runner uses lookup.
type taskRegistry struct {
	mu    sync.RWMutex
	tasks map[string]registered
}

func newTaskRegistry() *taskRegistry {
	return &taskRegistry{tasks: make(map[string]registered)}
}

func (r *taskRegistry) add(name string, def registered) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.tasks == nil {
		r.tasks = make(map[string]registered)
	}
	r.tasks[name] = def
}

func (r *taskRegistry) lookup(name string) (registered, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	def, ok := r.tasks[name]
	return def, ok
}
