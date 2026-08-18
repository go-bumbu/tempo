package tempo_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-bumbu/tempo"
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
