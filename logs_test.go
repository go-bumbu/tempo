package tempo_test

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-bumbu/tempo"
	"github.com/google/uuid"
)

// compile-time: MemTaskLogSink satisfies the optional interfaces
var (
	_ tempo.TaskLogReader  = (*tempo.MemTaskLogSink)(nil)
	_ tempo.TaskLogCleaner = (*tempo.MemTaskLogSink)(nil)
)

func TestMemSinkReadRemoveRetain(t *testing.T) {
	ctx := context.Background()
	sink := tempo.NewMemTaskLogSink()
	a, b := uuid.New(), uuid.New()
	mustAppend(t, sink, ctx, a, "INFO", "a1")
	mustAppend(t, sink, ctx, b, "INFO", "b1")

	got, err := sink.Logs(ctx, a)
	if err != nil || len(got) != 1 || got[0].Message != "a1" {
		t.Fatalf("Logs(a) = %+v, %v", got, err)
	}

	if err := sink.RemoveTasks(ctx, []uuid.UUID{a}); err != nil {
		t.Fatal(err)
	}
	if got, _ := sink.Logs(ctx, a); got != nil {
		t.Fatalf("after RemoveTasks, Logs(a) = %+v, want nil", got)
	}

	if err := sink.RetainOnly(ctx, []uuid.UUID{}); err != nil {
		t.Fatal(err)
	}
	if got, _ := sink.Logs(ctx, b); got != nil {
		t.Fatalf("after RetainOnly(none), Logs(b) = %+v, want nil", got)
	}
}

func mustAppend(t *testing.T, s tempo.TaskLogSink, ctx context.Context, id uuid.UUID, lvl, msg string) {
	t.Helper()
	if err := s.Append(ctx, id, lvl, msg); err != nil {
		t.Fatal(err)
	}
}

// TestRunnerPerTaskLogIsolation runs several tasks at once, each logging a line
// tagged with its own name, and asserts that every task's log bucket holds only
// its own lines. It guards the task-ID-from-context plumbing (logs.go): a
// regression that shared or crossed the id would leak one task's logs into
// another's bucket, which no existing test would catch.
func TestRunnerPerTaskLogIsolation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const n = 5
		sink := tempo.NewMemTaskLogSink()
		r := newTestRunner(tempo.RunnerCfg{Parallelism: n, QueueSize: 2 * n, LogSink: sink})
		for i := 0; i < n; i++ {
			name := fmt.Sprintf("task-%d", i)
			r.RegisterRaw(name, func(_ context.Context, log *slog.Logger, _ []byte) error {
				log.Info("hello from " + name)
				time.Sleep(1 * time.Minute)
				return nil
			})
		}
		r.StartBg()

		idToName := make(map[uuid.UUID]string, n)
		for i := 0; i < n; i++ {
			name := fmt.Sprintf("task-%d", i)
			id, err := r.AddRaw(name, nil)
			if err != nil {
				t.Fatal(err)
			}
			idToName[id] = name
		}

		// Let every task run concurrently and finish.
		time.Sleep(2 * time.Minute)
		if err := r.ShutDown(context.Background()); err != nil {
			t.Fatalf("shutdown: %v", err)
		}

		for id, name := range idToName {
			var msgs []string
			entries, err := sink.Logs(context.Background(), id)
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range entries {
				msgs = append(msgs, e.Message)
			}
			// The runner adds "task started"/"task finished" around the handler's
			// own line. Order is not the point here — isolation is — so compare as
			// sorted sets.
			sort.Strings(msgs)
			want := []string{"hello from " + name, "task finished", "task started"}
			sort.Strings(want)
			if strings.Join(msgs, "|") != strings.Join(want, "|") {
				t.Errorf("task %s (id %s): logs = %v, want %v", name, id, msgs, want)
			}
		}
	})
}

// TestRunnerLogLevelFiltering guards LogLevel: a handler log below the
// configured level must not reach the sink, while one at or above it must. The
// examples only ever configure LevelInfo, so the filtering path itself is
// otherwise unexercised.
func TestRunnerLogLevelFiltering(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sink := tempo.NewMemTaskLogSink()
		r := newTestRunner(tempo.RunnerCfg{
			Parallelism: 1,
			QueueSize:   5,
			LogSink:     sink,
			LogLevel:    slog.LevelWarn,
		})
		r.RegisterRaw("x", func(_ context.Context, log *slog.Logger, _ []byte) error {
			log.Info("info-should-be-dropped")
			log.Warn("warn-should-be-kept")
			return nil
		})
		r.StartBg()
		id, err := r.AddRaw("x", nil)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(1 * time.Minute)
		if err := r.ShutDown(context.Background()); err != nil {
			t.Fatalf("shutdown: %v", err)
		}

		var gotInfo, gotWarn bool
		entries, err := sink.Logs(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			switch e.Message {
			case "info-should-be-dropped":
				gotInfo = true
			case "warn-should-be-kept":
				gotWarn = true
			}
		}
		if gotInfo {
			t.Error("expected the INFO handler line to be filtered out at LogLevel=Warn")
		}
		if !gotWarn {
			t.Error("expected the WARN handler line to reach the sink at LogLevel=Warn")
		}
	})
}
