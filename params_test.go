package tempo_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-bumbu/tempo"
	"github.com/google/uuid"
)

func TestRegisterRawReceivesParams(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		got := make(chan []byte, 1)
		r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5})
		r.RegisterRaw("scan", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, params []byte) error {
			got <- params
			return nil
		})
		r.StartBg()

		if _, _, err := r.AddRaw("scan", []byte(`{"mode":"full"}`)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
		if err := r.ShutDown(context.Background()); err != nil {
			t.Fatalf("shutdown: %v", err)
		}

		select {
		case p := <-got:
			if string(p) != `{"mode":"full"}` {
				t.Errorf("handler params: got %q", p)
			}
		default:
			t.Fatal("handler did not run")
		}
	})
}

func TestRegisterRawMaxParallelism(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRunner(tempo.RunnerCfg{Parallelism: 3, QueueSize: 10})
		r.RegisterRaw("scan", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error {
			time.Sleep(10 * time.Minute)
			return nil
		}, tempo.WithMaxParallelism(1))
		r.StartBg()

		for i := 0; i < 3; i++ {
			if _, _, err := r.AddRaw("scan", nil); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(1 * time.Minute)

		running := 0
		for _, task := range r.List() {
			if task.Status == tempo.TaskStatusRunning {
				running++
			}
		}
		if running != 1 {
			t.Errorf("running with MaxParallelism 1: got %d want 1", running)
		}

		go func() {
			time.Sleep(2000 * time.Minute)
			_ = r.ShutDown(context.Background())
		}()
		r.Wait()
	})
}

// TestRegisterRawOverwriteWins guards the documented "overwrites any handler
// already registered for name" behavior of RegisterRaw.
func TestRegisterRawOverwriteWins(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ran := make(chan string, 1)
		r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5})
		r.RegisterRaw("x", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error {
			ran <- "first"
			return nil
		})
		r.RegisterRaw("x", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error {
			ran <- "second"
			return nil
		})
		r.StartBg()
		if _, _, err := r.AddRaw("x", nil); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1 * time.Minute)
		_ = r.ShutDown(context.Background())

		select {
		case got := <-ran:
			if got != "second" {
				t.Errorf("expected the second registration to win, got %q", got)
			}
		default:
			t.Fatal("handler did not run")
		}
	})
}

// TestEnqueueMarshalError guards that a payload that cannot be JSON-encoded
// fails the enqueue up front, rather than queueing a task that can never decode.
func TestEnqueueMarshalError(t *testing.T) {
	r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5})
	// A channel has no JSON representation.
	id, coalesced, err := tempo.Enqueue(r, "x", make(chan int))
	if err == nil {
		t.Fatal("expected a marshal error")
	}
	if id != uuid.Nil {
		t.Errorf("expected uuid.Nil on marshal error, got %v", id)
	}
	if coalesced {
		t.Errorf("expected coalesced false on marshal error, got true")
	}
	if got := len(r.List()); got != 0 {
		t.Errorf("expected nothing queued after a marshal error, got %d", got)
	}
}

type scanParams struct {
	Mode string `json:"mode"`
}

func TestEnqueueTypedRoundTrip(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		got := make(chan scanParams, 1)
		r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5})
		tempo.Register(r, "scan", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, p scanParams) error {
			got <- p
			return nil
		})
		r.StartBg()

		if _, _, err := tempo.Enqueue(r, "scan", scanParams{Mode: "full"}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
		_ = r.ShutDown(context.Background())

		select {
		case p := <-got:
			if p.Mode != "full" {
				t.Errorf("typed params: got %+v", p)
			}
		default:
			t.Fatal("handler did not run")
		}
	})
}

func TestRegisterTypedEmptyParams(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		got := make(chan scanParams, 1)
		r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5})
		tempo.Register(r, "scan", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, p scanParams) error {
			got <- p
			return nil
		})
		r.StartBg()

		if _, _, err := r.AddRaw("scan", nil); err != nil { // no payload
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
		_ = r.ShutDown(context.Background())

		select {
		case p := <-got:
			if p != (scanParams{}) {
				t.Errorf("expected zero value, got %+v", p)
			}
		default:
			t.Fatal("handler did not run")
		}
	})
}

func TestRegisterTypedMalformedFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5})
		tempo.Register(r, "scan", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, p scanParams) error {
			return nil
		})
		r.StartBg()

		id, _, err := r.AddRaw("scan", []byte(`{ not json`))
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
		_ = r.ShutDown(context.Background())

		info, err := r.GetTask(id)
		if err != nil {
			t.Fatalf("GetTask: %v", err)
		}
		if info.Status != tempo.TaskStatusFailed {
			t.Errorf("malformed params: got status %s want failed", info.Status.Str())
		}
	})
}

type recoverableMem struct {
	mu    sync.Mutex
	tasks map[uuid.UUID]tempo.TaskInfo
}

func newRecoverableMem() *recoverableMem {
	return &recoverableMem{tasks: make(map[uuid.UUID]tempo.TaskInfo)}
}

func (m *recoverableMem) SaveTask(ctx context.Context, t tempo.TaskInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tasks[t.ID] = t
	return nil
}

func (m *recoverableMem) RemoveTasks(ctx context.Context, ids []uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range ids {
		delete(m.tasks, id)
	}
	return nil
}

func (m *recoverableMem) List(ctx context.Context) ([]tempo.TaskInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]tempo.TaskInfo, 0, len(m.tasks))
	for _, t := range m.tasks {
		out = append(out, t)
	}
	return out, nil
}

func TestEnqueueTypedRecovered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		persist := newRecoverableMem()

		// Runner 1: enqueue but never start it, so the task stays Waiting in persistence.
		r1 := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5, Persistence: persist})
		if _, _, err := tempo.Enqueue(r1, "scan", scanParams{Mode: "full"}); err != nil {
			t.Fatal(err)
		}

		// Runner 2: recovers the waiting task from the same persistence and runs it.
		got := make(chan scanParams, 1)
		r2 := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5, Persistence: persist})
		tempo.Register(r2, "scan", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, p scanParams) error {
			got <- p
			return nil
		})
		r2.StartBg()
		time.Sleep(100 * time.Millisecond)
		_ = r2.ShutDown(context.Background())

		select {
		case p := <-got:
			if p.Mode != "full" {
				t.Errorf("recovered typed params: got %+v", p)
			}
		default:
			t.Fatal("recovered task did not run")
		}
	})
}

func TestWithSingletonCoalescesRaw(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 10})
		r.RegisterRaw("scan", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error {
			time.Sleep(10 * time.Minute)
			return nil
		}, tempo.WithSingleton())
		r.StartBg()

		ids := make([]uuid.UUID, 3)
		coalesced := make([]bool, 3)
		for i := range ids {
			id, c, err := r.AddRaw("scan", nil)
			if err != nil {
				t.Fatalf("AddRaw %d: %v", i, err)
			}
			ids[i] = id
			coalesced[i] = c
		}
		time.Sleep(1 * time.Minute) // let the worker claim the first
		synctest.Wait()

		if coalesced[0] {
			t.Errorf("enqueue 0: got coalesced true, want false (fresh insert)")
		}
		for i := 1; i < len(ids); i++ {
			if !coalesced[i] {
				t.Errorf("enqueue %d: got coalesced false, want true", i)
			}
			if ids[i] != ids[0] {
				t.Errorf("enqueue %d: got id %v want %v (coalesced)", i, ids[i], ids[0])
			}
		}
		tasks := r.List()
		if len(tasks) != 1 {
			t.Fatalf("task count: got %d want 1", len(tasks))
		}
		if tasks[0].Status != tempo.TaskStatusRunning {
			t.Errorf("status: got %s want running", tasks[0].Status.Str())
		}

		go func() {
			time.Sleep(2000 * time.Minute)
			_ = r.ShutDown(context.Background())
		}()
		r.Wait()
	})
}

func TestWithSingletonReleasesAfterTerminal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 10})
		r.RegisterRaw("scan", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error {
			return nil // completes immediately
		}, tempo.WithSingleton())
		r.StartBg()

		id1, coalesced1, err := r.AddRaw("scan", nil)
		if err != nil {
			t.Fatalf("AddRaw 1: %v", err)
		}
		if coalesced1 {
			t.Errorf("AddRaw 1: got coalesced true, want false (fresh insert)")
		}
		time.Sleep(1 * time.Minute) // let it run to completion
		synctest.Wait()

		id2, coalesced2, err := r.AddRaw("scan", nil)
		if err != nil {
			t.Fatalf("AddRaw 2: %v", err)
		}
		if coalesced2 {
			t.Errorf("AddRaw 2: got coalesced true, want false (new task after terminal)")
		}
		if id2 == id1 {
			t.Errorf("after completion expected a new task id, got the same %v", id1)
		}

		go func() {
			time.Sleep(2000 * time.Minute)
			_ = r.ShutDown(context.Background())
		}()
		r.Wait()
	})
}

func TestWithSingletonCoalescesTyped(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 10})
		// Different params on purpose: v1 dedups by task name, not by payload.
		tempo.Register(r, "scan", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ scanParams) error {
			time.Sleep(10 * time.Minute)
			return nil
		}, tempo.WithSingleton())
		r.StartBg()

		id1, coalesced1, err := tempo.Enqueue(r, "scan", scanParams{Mode: "full"})
		if err != nil {
			t.Fatalf("Enqueue 1: %v", err)
		}
		if coalesced1 {
			t.Errorf("Enqueue 1: got coalesced true, want false (fresh insert)")
		}
		id2, coalesced2, err := tempo.Enqueue(r, "scan", scanParams{Mode: "normal"})
		if err != nil {
			t.Fatalf("Enqueue 2: %v", err)
		}
		if !coalesced2 {
			t.Errorf("Enqueue 2: got coalesced false, want true")
		}
		if id2 != id1 {
			t.Errorf("typed singleton should coalesce: got %v want %v", id2, id1)
		}

		go func() {
			time.Sleep(2000 * time.Minute)
			_ = r.ShutDown(context.Background())
		}()
		r.Wait()
	})
}
