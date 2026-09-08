package filelog_test

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/go-bumbu/tempo"
	"github.com/go-bumbu/tempo/filelog"
	"github.com/google/uuid"
)

func TestAppendReadRoundTrip(t *testing.T) {
	ctx := context.Background()
	s, err := filelog.New(filelog.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	for _, m := range []string{"one", "two", "three"} {
		if err := s.Append(ctx, id, "INFO", m); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.Logs(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].Message != "one" || got[2].Message != "three" {
		t.Fatalf("Logs = %+v", got)
	}
	if got[0].Level != "INFO" || got[0].At.IsZero() {
		t.Fatalf("entry not fully populated: %+v", got[0])
	}
}

func TestLogsUnknownIDIsNil(t *testing.T) {
	s, _ := filelog.New(filelog.Config{Dir: t.TempDir()})
	got, err := s.Logs(context.Background(), uuid.New())
	if err != nil || got != nil {
		t.Fatalf("Logs(unknown) = %+v, %v; want nil, nil", got, err)
	}
}

func TestLogsToleratesTruncatedFinalLine(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := filelog.New(filelog.Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	// One clean, complete line.
	if err := s.Append(ctx, id, "INFO", "survived"); err != nil {
		t.Fatal(err)
	}
	// Then a crash/kill mid-Append leaves a partial JSON fragment with no
	// trailing newline. Simulate it by appending a truncated line directly.
	f, err := os.OpenFile(filepath.Join(dir, id.String()+".jsonl"), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"at":"2026-09-08T00:00:00Z","level":"INFO","msg":"tru`); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := s.Logs(ctx, id)
	if err != nil {
		t.Fatalf("Logs errored on a truncated final line: %v", err)
	}
	if len(got) != 1 || got[0].Message != "survived" {
		t.Fatalf("Logs = %+v; want the single intact entry", got)
	}
}

func TestLogsStillErrorsOnMidFileCorruption(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := filelog.New(filelog.Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	// A corrupt line in the *middle* of the file: it is followed by a newline,
	// so it was a complete write that is genuinely corrupt, not a torn tail.
	// This must stay an error, not be silently dropped.
	corrupt := []byte("{\"msg\":\"ok\"}\nnot json\n{\"msg\":\"after\"}\n")
	if err := os.WriteFile(filepath.Join(dir, id.String()+".jsonl"), corrupt, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Logs(ctx, id); err == nil {
		t.Fatal("Logs did not error on mid-file corruption; a bad non-final line must not be silently dropped")
	}
}

func TestConcurrentAppend(t *testing.T) {
	ctx := context.Background()
	s, _ := filelog.New(filelog.Config{Dir: t.TempDir()})
	// All goroutines write the SAME task id, so the per-id striped lock is
	// actually contended: this is what proves same-file writes are serialized,
	// rather than each goroutine quietly writing to its own untouched file.
	id := uuid.New()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if err := s.Append(ctx, id, "INFO", "line"); err != nil {
					t.Errorf("Append: %v", err)
				}
			}
		}()
	}
	wg.Wait()

	// The exact count is the real proof the lock serializes same-file writes:
	// -race alone can't catch this, since Append opens a fresh *os.File per
	// call, so concurrent same-path writes aren't an instrumented memory race.
	got, err := s.Logs(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 400 {
		t.Fatalf("Logs = %d entries, want 400 (20 goroutines x 20 appends)", len(got))
	}
}

func TestRemoveTasks(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, _ := filelog.New(filelog.Config{Dir: dir})
	id := uuid.New()
	_ = s.Append(ctx, id, "INFO", "x")
	if err := s.RemoveTasks(ctx, []uuid.UUID{id}); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Logs(ctx, id); got != nil {
		t.Fatalf("after RemoveTasks, Logs = %+v", got)
	}
	// removing a missing id is a no-op
	if err := s.RemoveTasks(ctx, []uuid.UUID{uuid.New()}); err != nil {
		t.Fatal(err)
	}
}

func TestRetainOnly(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, _ := filelog.New(filelog.Config{Dir: dir})
	keep, drop := uuid.New(), uuid.New()
	_ = s.Append(ctx, keep, "INFO", "k")
	_ = s.Append(ctx, drop, "INFO", "d")
	// a foreign file must be left untouched (filtered by extension, before the
	// uuid.Parse guard ever runs)
	foreign := filepath.Join(dir, "notes.txt")
	if err := os.WriteFile(foreign, []byte("hi"), 0o600); err != nil {
		t.Fatal(err)
	}
	// a .jsonl file whose stem is not a valid uuid must also survive: this one
	// passes the extension filter and exercises the uuid.Parse guard itself.
	scratch := filepath.Join(dir, "scratch.jsonl")
	if err := os.WriteFile(scratch, []byte("hi"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.RetainOnly(ctx, []uuid.UUID{keep}); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Logs(ctx, keep); len(got) != 1 {
		t.Fatalf("keep removed: %+v", got)
	}
	if got, _ := s.Logs(ctx, drop); got != nil {
		t.Fatalf("orphan survived: %+v", got)
	}
	if _, err := os.Stat(foreign); err != nil {
		t.Fatalf("foreign file was touched: %v", err)
	}
	if _, err := os.Stat(scratch); err != nil {
		t.Fatalf("non-uuid .jsonl file was touched: %v", err)
	}
}

// fakeRecoverable recovers a fixed task list, for the startup-sweep integration test.
type fakeRecoverable struct{ list []tempo.TaskInfo }

func (f *fakeRecoverable) SaveTask(context.Context, tempo.TaskInfo) error { return nil }
func (f *fakeRecoverable) RemoveTasks(context.Context, []uuid.UUID) error { return nil }
func (f *fakeRecoverable) List(context.Context) ([]tempo.TaskInfo, error) { return f.list, nil }

func TestOrphanSweptWhenRunnerConstructed(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, _ := filelog.New(filelog.Config{Dir: dir})
	keep, orphan := uuid.New(), uuid.New()
	_ = s.Append(ctx, keep, "INFO", "k")
	_ = s.Append(ctx, orphan, "INFO", "o")

	_, err := tempo.NewQueueRunner(tempo.RunnerCfg{
		QueueSize: 10, HistorySize: 10,
		Persistence: &fakeRecoverable{list: []tempo.TaskInfo{{ID: keep, Name: "t", Status: tempo.TaskStatusComplete}}},
		LogSink:     s,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Logs(ctx, keep); len(got) != 1 {
		t.Fatalf("recovered task's logs were swept: %+v", got)
	}
	if got, _ := s.Logs(ctx, orphan); got != nil {
		t.Fatalf("orphan not swept: %+v", got)
	}
}
