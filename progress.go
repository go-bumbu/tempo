package tempo

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
)

// ProgressState is the latest reported progress for one task. Progress is
// last-value-wins: each update replaces the previous state. Percent and ETA are
// computed from it and never stored, so they cannot drift.
type ProgressState struct {
	Done      int64
	Total     int64
	Stage     string
	UpdatedAt time.Time
}

// Percent reports Done/Total clamped to [0,1]. ok is false when Total <= 0.
func (p ProgressState) Percent() (float64, bool) {
	if p.Total <= 0 {
		return 0, false
	}
	f := float64(p.Done) / float64(p.Total)
	if f < 0 {
		f = 0
	}
	if f > 1 {
		f = 1
	}
	return f, true
}

// ETA estimates the time remaining by linear extrapolation from startedAt to
// UpdatedAt. ok is false when it cannot be computed — no Total, no Done yet, a
// zero startedAt, or a non-positive elapsed — and it is (0, true) once
// Done >= Total.
func (p ProgressState) ETA(startedAt time.Time) (time.Duration, bool) {
	if p.Total <= 0 || p.Done <= 0 || startedAt.IsZero() {
		return 0, false
	}
	if p.Done >= p.Total {
		return 0, true
	}
	elapsed := p.UpdatedAt.Sub(startedAt)
	if elapsed <= 0 {
		return 0, false
	}
	remaining := time.Duration(float64(elapsed) * float64(p.Total-p.Done) / float64(p.Done))
	return remaining, true
}

// TaskProgressSink receives progress updates for a task (write path). Set is an
// upsert keyed by task id: the latest ProgressState wins. Like TaskLogSink.Append
// it is called synchronously on the worker goroutine and must not block; the
// runner ignores its error. Implementations must be safe for concurrent use.
type TaskProgressSink interface {
	Set(ctx context.Context, taskID uuid.UUID, p ProgressState) error
}

// TaskProgressReader reads a task's latest progress back. Optional: a sink
// implements it when its progress is retrievable (e.g. for a UI). ok is false
// for an unknown id. Mirrors TaskLogReader.
type TaskProgressReader interface {
	Progress(ctx context.Context, taskID uuid.UUID) (ProgressState, bool, error)
}

// TaskProgressCleaner lets the runner reap a sink's progress. Optional.
//
//	RemoveTasks — steady-state trim: called with the ids CleanHistory just removed.
//	RetainOnly  — startup reconciliation: drop every task's progress except keep.
type TaskProgressCleaner interface {
	RemoveTasks(ctx context.Context, ids []uuid.UUID) error
	RetainOnly(ctx context.Context, keep []uuid.UUID) error
}

// MemTaskProgressSink is an in-memory TaskProgressSink. Safe for concurrent use.
// It is the progress twin of MemTaskLogSink.
type MemTaskProgressSink struct {
	mu    sync.Mutex
	state map[uuid.UUID]ProgressState
}

// NewMemTaskProgressSink returns a new in-memory task progress sink.
func NewMemTaskProgressSink() *MemTaskProgressSink {
	return &MemTaskProgressSink{state: make(map[uuid.UUID]ProgressState)}
}

// Set implements TaskProgressSink (upsert by task id).
func (m *MemTaskProgressSink) Set(_ context.Context, taskID uuid.UUID, p ProgressState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state[taskID] = p
	return nil
}

// Progress implements TaskProgressReader. ok is false for an unknown id.
func (m *MemTaskProgressSink) Progress(_ context.Context, taskID uuid.UUID) (ProgressState, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.state[taskID]
	return p, ok, nil
}

// RemoveTasks implements TaskProgressCleaner.
func (m *MemTaskProgressSink) RemoveTasks(_ context.Context, ids []uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range ids {
		delete(m.state, id)
	}
	return nil
}

// RetainOnly implements TaskProgressCleaner: drops every task's progress except keep.
func (m *MemTaskProgressSink) RetainOnly(_ context.Context, keep []uuid.UUID) error {
	keepSet := make(map[uuid.UUID]struct{}, len(keep))
	for _, id := range keep {
		keepSet[id] = struct{}{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for id := range m.state {
		if _, ok := keepSet[id]; !ok {
			delete(m.state, id)
		}
	}
	return nil
}

var (
	_ TaskProgressSink    = (*MemTaskProgressSink)(nil)
	_ TaskProgressReader  = (*MemTaskProgressSink)(nil)
	_ TaskProgressCleaner = (*MemTaskProgressSink)(nil)
)

// Progress is how a running job reports its progress. The runner hands one to
// every task handler. It is safe to call from multiple goroutines, and it is a
// no-op when the runner has no ProgressSink configured, so a job may always
// call it.
type Progress interface {
	SetTotal(total int64)
	Set(done int64)
	Inc(delta int64) int64
	SetStage(msg string)
}

// discardProgress backs a task whose runner has no ProgressSink configured.
var discardProgress Progress = discardReporter{}

type discardReporter struct{}

func (discardReporter) SetTotal(int64)  {}
func (discardReporter) Set(int64)       {}
func (discardReporter) Inc(int64) int64 { return 0 }
func (discardReporter) SetStage(string) {}

// sinkReporter is the per-task Progress the runner hands to a job. It accumulates
// Done/Total/Stage and forwards a full ProgressState snapshot to the sink on each
// change. It is the progress twin of sinkHandler.
type sinkReporter struct {
	sink   TaskProgressSink
	taskID uuid.UUID
	mu     sync.Mutex
	state  ProgressState
}

// newSinkReporter returns a Progress bound to one task id, forwarding to sink.
func newSinkReporter(sink TaskProgressSink, taskID uuid.UUID) Progress {
	return &sinkReporter{sink: sink, taskID: taskID}
}

func (r *sinkReporter) SetTotal(total int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.Total = total
	r.flushLocked()
}

func (r *sinkReporter) Set(done int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.Done = done
	r.flushLocked()
}

func (r *sinkReporter) Inc(delta int64) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.Done += delta
	r.flushLocked()
	return r.state.Done
}

func (r *sinkReporter) SetStage(msg string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.Stage = msg
	r.flushLocked()
}

// flushLocked stamps the time and forwards a snapshot; the caller holds r.mu, so
// snapshots reach the sink in call order. The sink's error is ignored, matching
// appendTaskLog.
func (r *sinkReporter) flushLocked() {
	r.state.UpdatedAt = time.Now()
	_ = r.sink.Set(context.Background(), r.taskID, r.state)
}

var (
	_ Progress = discardReporter{}
	_ Progress = (*sinkReporter)(nil)
)
