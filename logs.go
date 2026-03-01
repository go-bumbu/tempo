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

// used to isolate they keys used in the context data
type taskIDCtxKey struct{}
type taskLoggerCtxKey struct{}

var (
	taskIDKey     = taskIDCtxKey{}
	taskLoggerKey = taskLoggerCtxKey{}
)

type sinkHandler struct {
	sink     TaskLogSink
	minLevel slog.Level
}

// NewSinkHandler returns a slog.Handler that forwards records to the sink using the task ID from context.
// Only records with level >= minLevel are sent. Use slog.LevelInfo (default 0) for info and above.
func NewSinkHandler(sink TaskLogSink, minLevel slog.Level) slog.Handler {
	return &sinkHandler{sink: sink, minLevel: minLevel}
}

func (h *sinkHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= h.minLevel
}

func (h *sinkHandler) Handle(ctx context.Context, r slog.Record) error {
	id, ok := ctx.Value(taskIDKey).(uuid.UUID)
	if !ok {
		return nil
	}
	return h.sink.Append(ctx, id, r.Level.String(), r.Message)
}

func (h *sinkHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h
}

func (h *sinkHandler) WithGroup(name string) slog.Handler {
	return h
}

// Logger returns the task-scoped logger from ctx if set (when running under a runner with LogSink).
// Otherwise returns a discard logger so nothing is logged. Use the Context methods or the level
// wrappers (Debug, Info, Warn, Error) so logs are associated with the task.
func Logger(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(taskLoggerKey).(*slog.Logger); ok && l != nil {
		return l
	}
	return discardLogger
}

// Debug logs at DEBUG. Pass ctx so the log is associated with the current task when LogSink is set.
func Debug(ctx context.Context, msg string, args ...any) {
	Logger(ctx).DebugContext(ctx, msg, args...)
}

// Info logs at INFO. Pass ctx so the log is associated with the current task when LogSink is set.
func Info(ctx context.Context, msg string, args ...any) {
	Logger(ctx).InfoContext(ctx, msg, args...)
}

// Warn logs at WARN. Pass ctx so the log is associated with the current task when LogSink is set.
func Warn(ctx context.Context, msg string, args ...any) {
	Logger(ctx).WarnContext(ctx, msg, args...)
}

// Error logs at ERROR. Pass ctx so the log is associated with the current task when LogSink is set.
func Error(ctx context.Context, msg string, args ...any) {
	Logger(ctx).ErrorContext(ctx, msg, args...)
}

// discardLogger is used when no task-scoped logger is in context; it drops all log output.
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

// Logs returns log entries for the given task ID, in order. Nil if none.
func (m *MemTaskLogSink) Logs(taskID uuid.UUID) []LogEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]LogEntry(nil), m.entries[taskID]...)
}
