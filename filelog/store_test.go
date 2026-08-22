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

func TestConcurrentAppend(t *testing.T) {
	ctx := context.Background()
	s, _ := filelog.New(filelog.Config{Dir: t.TempDir()})
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := uuid.New()
			for j := 0; j < 20; j++ {
				_ = s.Append(ctx, id, "INFO", "line")
			}
		}()
	}
	wg.Wait()
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
	// a foreign file must be left untouched
	foreign := filepath.Join(dir, "notes.txt")
	if err := os.WriteFile(foreign, []byte("hi"), 0o600); err != nil {
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
