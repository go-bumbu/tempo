package tempo_test

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-bumbu/tempo"
)

func TestGroupRunner_Run_noTasks(t *testing.T) {
	rg := tempo.NewGroupRunner()
	err := rg.Run()
	if err == nil {
		t.Fatal("expected error when Run with no tasks")
	}
	if err.Error() != "tempo: no tasks added" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestGroupRunner_Run_Stop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rg := tempo.NewGroupRunner()
		var ran bool
		rg.Add(tempo.TaskDef{
			Name: "worker",
			Run: func(ctx context.Context) error {
				ran = true
				<-ctx.Done()
				return ctx.Err()
			},
		})

		done := make(chan error, 1)
		go func() {
			done <- rg.Run()
		}()

		time.Sleep(50 * time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := rg.Stop(ctx); err != nil {
			t.Errorf("Stop: %v", err)
		}
		if err := <-done; err != nil {
			t.Errorf("Run returned: %v", err)
		}
		if !ran {
			t.Error("task did not run")
		}
	})
}

func TestGroupRunner_multipleTasks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rg := tempo.NewGroupRunner()
		var mu sync.Mutex
		done := make([]string, 0, 2)
		for _, name := range []string{"a", "b"} {
			name := name
			rg.Add(tempo.TaskDef{
				Name: name,
				Run: func(ctx context.Context) error {
					<-ctx.Done()
					mu.Lock()
					done = append(done, name)
					mu.Unlock()
					return ctx.Err()
				},
			})
		}

		runDone := make(chan error, 1)
		go func() {
			runDone <- rg.Run()
		}()

		time.Sleep(50 * time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := rg.Stop(ctx); err != nil {
			t.Errorf("Stop: %v", err)
		}
		if err := <-runDone; err != nil {
			t.Errorf("Run returned: %v", err)
		}
		mu.Lock()
		got := done
		mu.Unlock()
		// Order is non-deterministic (parallel execution)
		want := []string{"a", "b"}
		if len(got) != len(want) {
			t.Errorf("tasks done: got %v, want %v", got, want)
		} else {
			gotSet := make(map[string]bool)
			for _, s := range got {
				gotSet[s] = true
			}
			for _, s := range want {
				if !gotSet[s] {
					t.Errorf("tasks done: got %v, missing %q", got, s)
				}
			}
		}
	})
}

func TestGroupRunner_StopIdempotent(t *testing.T) {
	rg := tempo.NewGroupRunner()
	rg.Add(tempo.TaskDef{
		Name: "worker",
		Run: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
	})

	go func() { _ = rg.Run() }()
	time.Sleep(50 * time.Millisecond)

	ctx := context.Background()
	err1 := rg.Stop(ctx)
	err2 := rg.Stop(ctx)
	if err1 != err2 {
		t.Errorf("Stop twice: first %v, second %v", err1, err2)
	}
}

func TestGroupRunner_StopBeforeRun(t *testing.T) {
	rg := tempo.NewGroupRunner()
	rg.Add(tempo.TaskDef{
		Name: "worker",
		Run: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
	})

	// Stop before Run: qr is still nil, Stop closes blockCh and sends to stopResult.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := rg.Stop(ctx); err != nil {
		t.Errorf("Stop before Run: %v", err)
	}

	// Run should return immediately (blockCh already closed).
	err := rg.Run()
	if err != nil {
		t.Errorf("Run after Stop: %v", err)
	}
}

func TestGroupRunner_StopPropagatesShutdownError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rg := tempo.NewGroupRunner()
		rg.Add(tempo.TaskDef{
			Name: "worker",
			Run: func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			},
		})

		runDone := make(chan error, 1)
		go func() {
			runDone <- rg.Run()
		}()

		time.Sleep(50 * time.Millisecond)
		// Use cancelled context so ShutDown returns an error
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		stopErr := rg.Stop(ctx)
		if stopErr == nil {
			t.Fatal("expected Stop to return error when context cancelled")
		}
		runErr := <-runDone
		if runErr == nil {
			t.Fatal("expected Run to return same error as Stop")
		}
		// Run returns the same error sent by Stop
		if runErr != stopErr {
			t.Errorf("Run returned %v, Stop returned %v", runErr, stopErr)
		}
	})
}
