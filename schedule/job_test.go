package schedule

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
)

// enqueued records one AddRaw call.
type enqueued struct {
	name   string
	params []byte
}

// fakeEnqueuer records AddRaw calls and can be told to fail. Used by every test
// in this package that needs an Enqueuer. coalesce defaults to false, so every
// existing test (which never sets it) keeps observing a fresh enqueue.
type fakeEnqueuer struct {
	mu       sync.Mutex
	calls    []enqueued
	err      error
	coalesce bool
}

func (f *fakeEnqueuer) AddRaw(name string, params []byte) (uuid.UUID, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return uuid.Nil, false, f.err
	}
	f.calls = append(f.calls, enqueued{name: name, params: params})
	return uuid.New(), f.coalesce, nil
}

func (f *fakeEnqueuer) snapshot() []enqueued {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]enqueued(nil), f.calls...)
}

func (f *fakeEnqueuer) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

// quietLogger discards output so tests do not spam the console.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// captureLogger is quietLogger's counterpart for tests that need to inspect
// what was logged: same slog.NewTextHandler construction, but writing to a
// buffer instead of discarding.
func captureLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, nil)), &buf
}

func TestEnqueueJobExecute(t *testing.T) {
	ctx := context.Background()

	t.Run("enqueues the task name and params", func(t *testing.T) {
		enq := &fakeEnqueuer{}
		job := &enqueueJob{
			schedID:  uuid.New(),
			taskName: "scan",
			params:   []byte(`{"full":true}`),
			enq:      enq,
			log:      quietLogger(),
		}
		if err := job.Execute(ctx); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		calls := enq.snapshot()
		if len(calls) != 1 {
			t.Fatalf("expected 1 enqueue, got %d", len(calls))
		}
		if calls[0].name != "scan" {
			t.Errorf("expected task name \"scan\", got %q", calls[0].name)
		}
		if string(calls[0].params) != `{"full":true}` {
			t.Errorf("expected params to be passed through, got %s", calls[0].params)
		}
	})

	t.Run("hands out a fresh copy of params on every fire", func(t *testing.T) {
		enq := &fakeEnqueuer{}
		params := []byte(`{"full":true}`)
		job := &enqueueJob{schedID: uuid.New(), taskName: "scan", params: params, enq: enq, log: quietLogger()}

		if err := job.Execute(ctx); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// tempo does not copy the params slice, so a raw handler mutating what it
		// received must not corrupt the next fire.
		first := enq.snapshot()[0].params
		first[2] = 'X'

		if err := job.Execute(ctx); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		second := enq.snapshot()[1].params
		if string(second) != `{"full":true}` {
			t.Errorf("second fire saw mutated params: %s", second)
		}
		if string(params) != `{"full":true}` {
			t.Errorf("the job's own params were mutated: %s", params)
		}
	})

	t.Run("nil params stay nil", func(t *testing.T) {
		enq := &fakeEnqueuer{}
		job := &enqueueJob{schedID: uuid.New(), taskName: "scan", params: nil, enq: enq, log: quietLogger()}
		if err := job.Execute(ctx); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got := enq.snapshot()[0].params; got != nil {
			t.Errorf("expected nil params, got %v", got)
		}
	})

	t.Run("returns the enqueue error", func(t *testing.T) {
		wantErr := errors.New("queue full")
		enq := &fakeEnqueuer{}
		enq.setErr(wantErr)
		job := &enqueueJob{schedID: uuid.New(), taskName: "scan", enq: enq, log: quietLogger()}
		if err := job.Execute(ctx); !errors.Is(err, wantErr) {
			t.Errorf("expected the enqueue error, got %v", err)
		}
	})
}

// TestEnqueueJobSkipsOnCoalesce guards the coalesce branch of Execute: a fire
// that coalesces onto an already waiting/running WithSingleton task must be
// logged as a skip and return nil, without also logging the normal "schedule
// fired" line.
func TestEnqueueJobSkipsOnCoalesce(t *testing.T) {
	enq := &fakeEnqueuer{coalesce: true}
	log, buf := captureLogger()
	job := &enqueueJob{
		schedID:  uuid.New(),
		taskName: "scan",
		params:   []byte(`{"full":true}`),
		enq:      enq,
		log:      log,
	}

	if err := job.Execute(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "level=INFO") {
		t.Errorf("expected an Info line, got log output:\n%s", out)
	}
	if !strings.Contains(out, `msg="schedule fire skipped: task already waiting or running"`) {
		t.Errorf("expected the skip message, got log output:\n%s", out)
	}
	if !strings.Contains(out, "component=tempo/schedule") {
		t.Errorf("expected the component field, got log output:\n%s", out)
	}
	if !strings.Contains(out, "scheduleId="+job.schedID.String()) {
		t.Errorf("expected the scheduleId field, got log output:\n%s", out)
	}
	if !strings.Contains(out, "task=scan") {
		t.Errorf("expected the task field, got log output:\n%s", out)
	}
	if strings.Contains(out, "schedule fired") {
		t.Errorf("expected no \"schedule fired\" line on coalesce, got log output:\n%s", out)
	}
}

func TestEnqueueJobDescription(t *testing.T) {
	job := &enqueueJob{schedID: uuid.New(), taskName: "scan"}
	if got := job.Description(); got == "" {
		t.Error("expected a non-empty description")
	}
}
