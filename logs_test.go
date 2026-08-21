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
			r.RegisterRaw(name, func(ctx context.Context, _ []byte) error {
				tempo.Info(ctx, "hello from "+name)
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
			for _, e := range sink.Logs(id) {
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
		r.RegisterRaw("x", func(ctx context.Context, _ []byte) error {
			tempo.Info(ctx, "info-should-be-dropped")
			tempo.Warn(ctx, "warn-should-be-kept")
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
		for _, e := range sink.Logs(id) {
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
