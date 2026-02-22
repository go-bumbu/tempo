package tempo

import (
	"context"
	"sync"
)

// TaskDef defines how to run a task and optional per-task behavior.
type TaskDef struct {
	// Run is the function to execute for this task.
	Run func(ctx context.Context) error
	// MaxParallelism is the max number of this task name that may run at once.
	// 0 means no per-task limit (use runner default).
	MaxParallelism int
}

// TaskRegistry is the interface for registering and looking up task definitions.
// The runner uses it to resolve task name -> TaskDef when executing.
type TaskRegistry interface {
	Add(name string, def TaskDef)
	Remove(name string)
	Lookup(name string) (TaskDef, bool)
}

// taskRegistry is the default in-memory implementation of TaskRegistry.
type taskRegistry struct {
	mu    sync.RWMutex
	tasks map[string]TaskDef
}

// NewTaskRegistry creates an empty registry.
func NewTaskRegistry() TaskRegistry {
	return &taskRegistry{
		tasks: make(map[string]TaskDef),
	}
}

func (r *taskRegistry) Add(name string, def TaskDef) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.tasks == nil {
		r.tasks = make(map[string]TaskDef)
	}
	r.tasks[name] = def
}

func (r *taskRegistry) Remove(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.tasks, name)
}

func (r *taskRegistry) Lookup(name string) (TaskDef, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	def, ok := r.tasks[name]
	return def, ok
}
