package tempo

import (
	"context"
	"sync"
)

// TaskDef defines how to run a task and optional per-task behavior.
type TaskDef struct {
	// Name identifies the task (used for registration and enqueueing).
	Name string
	// Run is the function to execute for this task.
	Run func(ctx context.Context) error
	// MaxParallelism is the max number of this task name that may run at once.
	// 0 means no per-task limit (use runner default).
	MaxParallelism int
}

// taskRegistry is the internal in-memory registry; only the runner uses lookup.
type taskRegistry struct {
	mu    sync.RWMutex
	tasks map[string]TaskDef
}

func newTaskRegistry() *taskRegistry {
	return &taskRegistry{tasks: make(map[string]TaskDef)}
}

func (r *taskRegistry) add(def TaskDef) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.tasks == nil {
		r.tasks = make(map[string]TaskDef)
	}
	r.tasks[def.Name] = def
}

func (r *taskRegistry) lookup(name string) (TaskDef, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	def, ok := r.tasks[name]
	return def, ok
}
