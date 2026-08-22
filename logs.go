package tempo

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
)

// TaskLogSink receives log lines for a task. Implementations may write to DB, memory, or a log service.
// Append is called from the runner's slog handler and may be invoked concurrently from multiple
// tasks (or multiple log lines from the same task). Custom implementations must be thread-safe:
// use a mutex, atomic operations, or serialize writes so that concurrent Append calls do not race.
// Errors returned by Append are silently ignored by the runner; handle or log failures inside the implementation if needed.
type TaskLogSink interface {
	Append(ctx context.Context, taskID uuid.UUID, level string, msg string) error
}

// TaskLogReader reads a task's log lines back. Optional: a sink implements it
// when its logs are retrievable (e.g. for a UI). Mirrors the way
// RecoverablePersistence optionally extends TaskStatePersistence.
type TaskLogReader interface {
	Logs(ctx context.Context, taskID uuid.UUID) ([]LogEntry, error)
}

// TaskLogCleaner lets the runner reap a sink's logs. Optional.
//
//	RemoveTasks — steady-state trim: called with the ids CleanHistory just removed.
//	RetainOnly  — startup reconciliation: delete every task's logs except keep.
type TaskLogCleaner interface {
	RemoveTasks(ctx context.Context, ids []uuid.UUID) error
	RetainOnly(ctx context.Context, keep []uuid.UUID) error
}

type sinkHandler struct {
	sink     TaskLogSink
	minLevel slog.Level
	taskID   uuid.UUID
	attrs    []slog.Attr // accumulated via WithAttrs, already group-qualified
	group    string      // dotted prefix from WithGroup
}

// newSinkHandler returns a slog.Handler bound to one task id; each record is
// forwarded to the sink. Only records with level >= minLevel are sent.
func newSinkHandler(sink TaskLogSink, minLevel slog.Level, taskID uuid.UUID) slog.Handler {
	return &sinkHandler{sink: sink, minLevel: minLevel, taskID: taskID}
}

func (h *sinkHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.minLevel
}

func (h *sinkHandler) Handle(ctx context.Context, r slog.Record) error {
	msg := r.Message
	for _, a := range h.attrs {
		msg += " " + a.Key + "=" + a.Value.String()
	}
	r.Attrs(func(a slog.Attr) bool {
		key := a.Key
		if h.group != "" {
			key = h.group + "." + key
		}
		msg += " " + key + "=" + a.Value.String()
		return true
	})
	return h.sink.Append(ctx, h.taskID, r.Level.String(), msg)
}

func (h *sinkHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	nh := *h
	nh.attrs = append([]slog.Attr(nil), h.attrs...)
	for _, a := range attrs {
		if h.group != "" {
			a.Key = h.group + "." + a.Key
		}
		nh.attrs = append(nh.attrs, a)
	}
	return &nh
}

func (h *sinkHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	nh := *h
	if h.group == "" {
		nh.group = name
	} else {
		nh.group = h.group + "." + name
	}
	return &nh
}

// discardLogger backs a task whose runner has no LogSink configured.
var discardLogger = slog.New(&discardHandler{})

type discardHandler struct{}

func (*discardHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (*discardHandler) Handle(context.Context, slog.Record) error { return nil }
func (h *discardHandler) WithAttrs([]slog.Attr) slog.Handler      { return h }
func (h *discardHandler) WithGroup(string) slog.Handler           { return h }

// LogEntry is a single task log line. Used by MemTaskLogSink for retrieval.
type LogEntry struct {
	Level   string
	Message string
	At      time.Time
}

// MemTaskLogSink is an in-memory TaskLogSink. Safe for concurrent use. Use Logs to retrieve by task ID.
type MemTaskLogSink struct {
	mu      sync.Mutex
	entries map[uuid.UUID][]LogEntry
}

// NewMemTaskLogSink returns a new in-memory task log sink.
func NewMemTaskLogSink() *MemTaskLogSink {
	return &MemTaskLogSink{entries: make(map[uuid.UUID][]LogEntry)}
}

// Append implements TaskLogSink.
func (m *MemTaskLogSink) Append(ctx context.Context, taskID uuid.UUID, level string, msg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries[taskID] = append(m.entries[taskID], LogEntry{Level: level, Message: msg, At: time.Now()})
	return nil
}

// Logs implements TaskLogReader. Returns nil for an unknown id.
func (m *MemTaskLogSink) Logs(_ context.Context, taskID uuid.UUID) ([]LogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.entries[taskID]) == 0 {
		return nil, nil
	}
	return append([]LogEntry(nil), m.entries[taskID]...), nil
}

// RemoveTasks implements TaskLogCleaner.
func (m *MemTaskLogSink) RemoveTasks(_ context.Context, ids []uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range ids {
		delete(m.entries, id)
	}
	return nil
}

// RetainOnly implements TaskLogCleaner: drops every task's logs except keep.
func (m *MemTaskLogSink) RetainOnly(_ context.Context, keep []uuid.UUID) error {
	keepSet := make(map[uuid.UUID]struct{}, len(keep))
	for _, id := range keep {
		keepSet[id] = struct{}{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for id := range m.entries {
		if _, ok := keepSet[id]; !ok {
			delete(m.entries, id)
		}
	}
	return nil
}

var (
	_ TaskLogSink    = (*MemTaskLogSink)(nil)
	_ TaskLogReader  = (*MemTaskLogSink)(nil)
	_ TaskLogCleaner = (*MemTaskLogSink)(nil)
)
