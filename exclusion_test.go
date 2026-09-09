package tempo

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// concurrencyProbe is a task handler that records the peak number of its own
// concurrent executions. Each run holds for `hold` so any overlap is
// observable, and calls wg.Done() on exit.
func concurrencyProbe(active, maxActive *int32, hold time.Duration, wg *sync.WaitGroup) func(context.Context, *slog.Logger, Progress, []byte) error {
	return func(ctx context.Context, log *slog.Logger, prog Progress, params []byte) error {
		defer wg.Done()
		n := atomic.AddInt32(active, 1)
		for {
			m := atomic.LoadInt32(maxActive)
			if n <= m || atomic.CompareAndSwapInt32(maxActive, m, n) {
				break
			}
		}
		time.Sleep(hold)
		atomic.AddInt32(active, -1)
		return nil
	}
}

func TestExclusionGroup_SerializesAcrossNames(t *testing.T) {
	r, err := NewQueueRunner(RunnerCfg{Parallelism: 4, QueueSize: 10, Persistence: NewMemPersistence()})
	if err != nil {
		t.Fatal(err)
	}
	var active, maxActive int32
	var wg sync.WaitGroup
	h := concurrencyProbe(&active, &maxActive, 100*time.Millisecond, &wg)
	r.RegisterRaw("scan", h, WithExclusionGroup("files"))
	r.RegisterRaw("reindex", h, WithExclusionGroup("files"))

	r.StartBg()
	wg.Add(2)
	if _, _, err := r.AddRaw("scan", nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.AddRaw("reindex", nil); err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	if got := atomic.LoadInt32(&maxActive); got != 1 {
		t.Fatalf("exclusion group allowed %d concurrent runs, want 1", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.ShutDown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestTasksWithoutGroupOverlap(t *testing.T) {
	r, err := NewQueueRunner(RunnerCfg{Parallelism: 4, QueueSize: 10, Persistence: NewMemPersistence()})
	if err != nil {
		t.Fatal(err)
	}
	var active, maxActive int32
	var wg sync.WaitGroup
	h := concurrencyProbe(&active, &maxActive, 100*time.Millisecond, &wg)
	r.RegisterRaw("a", h)
	r.RegisterRaw("b", h)

	r.StartBg()
	wg.Add(2)
	if _, _, err := r.AddRaw("a", nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.AddRaw("b", nil); err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	if got := atomic.LoadInt32(&maxActive); got < 2 {
		t.Fatalf("ungrouped tasks did not overlap: peak concurrency %d, want >= 2", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.ShutDown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestExclusionGroup_DoesNotBlockUnrelatedTasks(t *testing.T) {
	r, err := NewQueueRunner(RunnerCfg{Parallelism: 4, QueueSize: 10, Persistence: NewMemPersistence()})
	if err != nil {
		t.Fatal(err)
	}

	reindexStarted := make(chan struct{})
	releaseReindex := make(chan struct{})
	r.RegisterRaw("reindex", func(ctx context.Context, log *slog.Logger, prog Progress, params []byte) error {
		close(reindexStarted)
		<-releaseReindex
		return nil
	}, WithExclusionGroup("files"))
	r.RegisterRaw("scan", func(ctx context.Context, log *slog.Logger, prog Progress, params []byte) error {
		return nil
	}, WithExclusionGroup("files"))

	otherDone := make(chan struct{})
	r.RegisterRaw("other", func(ctx context.Context, log *slog.Logger, prog Progress, params []byte) error {
		close(otherDone)
		return nil
	})

	r.StartBg()
	if _, _, err := r.AddRaw("reindex", nil); err != nil {
		t.Fatal(err)
	}
	<-reindexStarted                                    // reindex now holds group "files"
	if _, _, err := r.AddRaw("scan", nil); err != nil { // blocked by the group
		t.Fatal(err)
	}
	if _, _, err := r.AddRaw("other", nil); err != nil { // must still run
		t.Fatal(err)
	}

	select {
	case <-otherDone:
		// success: an unrelated task ran while the group was held
	case <-time.After(2 * time.Second):
		t.Fatal("ungrouped task was blocked while the exclusion group was held")
	}

	close(releaseReindex)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.ShutDown(ctx); err != nil {
		t.Fatal(err)
	}
}
