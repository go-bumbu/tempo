package tempo_test

import (
	"context"
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
		r.RegisterRaw("scan", func(ctx context.Context, params []byte) error {
			got <- params
			return nil
		})
		r.StartBg()

		if _, err := r.AddRaw("scan", []byte(`{"mode":"full"}`)); err != nil {
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
		r.RegisterRaw("scan", func(ctx context.Context, _ []byte) error {
			time.Sleep(10 * time.Minute)
			return nil
		}, tempo.WithMaxParallelism(1))
		r.StartBg()

		for i := 0; i < 3; i++ {
			if _, err := r.AddRaw("scan", nil); err != nil {
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

type scanParams struct {
	Mode string `json:"mode"`
}

func TestEnqueueTypedRoundTrip(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		got := make(chan scanParams, 1)
		r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5})
		tempo.Register(r, "scan", func(ctx context.Context, p scanParams) error {
			got <- p
			return nil
		})
		r.StartBg()

		if _, err := tempo.Enqueue(r, "scan", scanParams{Mode: "full"}); err != nil {
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
		tempo.Register(r, "scan", func(ctx context.Context, p scanParams) error {
			got <- p
			return nil
		})
		r.StartBg()

		if _, err := r.AddRaw("scan", nil); err != nil { // no payload
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
		tempo.Register(r, "scan", func(ctx context.Context, p scanParams) error {
			return nil
		})
		r.StartBg()

		id, err := r.AddRaw("scan", []byte(`{ not json`))
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
		if _, err := tempo.Enqueue(r1, "scan", scanParams{Mode: "full"}); err != nil {
			t.Fatal(err)
		}

		// Runner 2: recovers the waiting task from the same persistence and runs it.
		got := make(chan scanParams, 1)
		r2 := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5, Persistence: persist})
		tempo.Register(r2, "scan", func(ctx context.Context, p scanParams) error {
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
