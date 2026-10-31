package tempo_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-bumbu/tempo"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"
)

// newTestRunner creates a QueueRunner; if cfg.Persistence is nil, in-memory persistence is used. Use r.RegisterRaw to add task definitions.
func newTestRunner(cfg tempo.RunnerCfg) *tempo.QueueRunner {
	if cfg.HistorySize == 0 {
		cfg.HistorySize = 10
	}
	if cfg.Persistence == nil {
		cfg.Persistence = tempo.NewMemPersistence()
	}
	r, err := tempo.NewQueueRunner(cfg)
	if err != nil {
		panic(err)
	}
	return r
}

func TestTaskData(t *testing.T) {

	synctest.Test(t, func(t *testing.T) {
		r := newTestRunner(tempo.RunnerCfg{Parallelism: 3, QueueSize: 20})
		r.RegisterRaw("some action", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error {
			time.Sleep(10 * time.Minute)
			return nil
		})
		r.StartBg()

		_, _, err := r.AddRaw("some action", nil)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)

		err = r.ShutDown(context.Background())
		if err != nil {
			t.Fatalf("unable to shut down server: %v", err)
		}
		want := tempo.TaskInfo{
			Name:      "some action",
			Status:    tempo.TaskStatusComplete,
			QueuedAt:  getTime("2000-01-01 01:00:00 +0100 CET"),
			StartedAt: getTime("2000-01-01 01:00:00 +0100 CET"),
			EndedAt:   getTime("2000-01-01 01:10:00 +0100 CET"),
		}

		got := r.List()
		if diff := cmp.Diff(got[0], want, cmpopts.IgnoreFields(tempo.TaskInfo{}, "ID")); diff != "" {
			t.Errorf("unexpected value (-got +want)\n%s", diff)
		}
	})
}

func getTime(in string) time.Time {
	layout := "2006-01-02 15:04:05 -0700 MST"
	t, err := time.Parse(layout, in)
	if err != nil {
		return time.Time{}
	}
	return t
}
func TestRunnerParallelism(t *testing.T) {

	t.Run("run waitingTasks sequentially with parallelism 1", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var result []string
			lock := sync.Mutex{}
			r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 10})
			for i := 1; i <= 4; i++ {
				n := i
				name := strconv.Itoa(n)
				r.RegisterRaw(name, func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error {
					lock.Lock()
					result = append(result, strconv.Itoa(n))
					lock.Unlock()
					time.Sleep(10 * time.Minute)
					return nil
				})
			}
			r.StartBg()

			for i := 1; i <= 4; i++ {
				_, _, err := r.AddRaw(strconv.Itoa(i), nil)
				if err != nil {
					t.Fatal(err)
				}
				time.Sleep(200 * time.Millisecond)
			}

			go func() {
				// wait before running shutdown, this simulates a signal listener like
				// signal.Notify(make(chan os.Signal, 1), syscall.SIGINT, syscall.SIGTERM)
				time.Sleep(2000 * time.Minute)
				//spew.Dump("trigger shutdown")
				err := r.ShutDown(context.Background())
				if err != nil {
					t.Errorf("unable to shut down server: %v", err)
				}
			}()
			r.Wait()

			want := []string{"1", "2", "3", "4"}
			if diff := cmp.Diff(result, want); diff != "" {
				t.Errorf("unexpected value (-got +want)\n%s", diff)
			}
		})
	})

	t.Run("run all waitingTasks with parallelism 3", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var result []string
			lock := sync.Mutex{}
			r := newTestRunner(tempo.RunnerCfg{Parallelism: 3, QueueSize: 20})
			for i := 1; i <= 12; i++ {
				n := i
				name := strconv.Itoa(n)
				r.RegisterRaw(name, func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error {
					lock.Lock()
					result = append(result, strconv.Itoa(n))
					lock.Unlock()
					time.Sleep(10 * time.Minute)
					return nil
				})
			}
			r.StartBg()

			for i := 1; i <= 12; i++ {
				_, _, err := r.AddRaw(strconv.Itoa(i), nil)
				if err != nil {
					t.Fatal(err)
				}
			}

			go func() {
				// wait before running shutdown, this simulates a signal listener like
				// signal.Notify(make(chan os.Signal, 1), syscall.SIGINT, syscall.SIGTERM)
				time.Sleep(2000 * time.Minute)
				//spew.Dump("trigger shutdown")
				err := r.ShutDown(context.Background())
				if err != nil {
					t.Errorf("unable to shut down server: %v", err)
				}
			}()
			r.Wait()

			want := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"}
			// sort the result, to ensure all jobs were executed
			sort.Slice(result, func(i, j int) bool {
				a, _ := strconv.Atoi(result[i])
				b, _ := strconv.Atoi(result[j])
				return a < b
			})

			if diff := cmp.Diff(result, want); diff != "" {
				t.Errorf("unexpected value (-got +want)\n%s", diff)
			}
		})
	})
}

func TestRunnerLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5})
		r.RegisterRaw("some action", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error {
			time.Sleep(10 * time.Minute)
			return nil
		})
		r.StartBg()

		_, _, err := r.AddRaw("some action", nil)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)

		for i := 1; i <= 5; i++ {
			_, _, err := r.AddRaw("some action", nil)
			if err != nil {
				t.Fatal(err)
			}
		}

		_, _, err = r.AddRaw("some action", nil)

		if !errors.Is(err, tempo.ErrQueueFull) {
			t.Errorf("expect err to be tempo.ErrQueueFull but got %v", err)
		}

		err = r.ShutDown(context.Background())
		if err != nil {
			t.Fatalf("unable to shut down server: %v", err)
		}

	})
}

func TestRunnerHistoryClean(t *testing.T) {
	t.Run("big history", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5, HistorySize: 50})
			r.RegisterRaw("success", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error {
				time.Sleep(1 * time.Minute)
				return nil
			})
			r.RegisterRaw("fail", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error {
				time.Sleep(1 * time.Minute)
				return fmt.Errorf("fail task")
			})
			r.StartBg()

			for i := 1; i <= 10; i++ {
				_, _, err := r.AddRaw("success", nil)
				if err != nil {
					t.Fatal(err)
				}
				time.Sleep(70 * time.Second)
			}
			for i := 1; i <= 5; i++ {
				_, _, err := r.AddRaw("fail", nil)
				if err != nil {
					t.Fatal(err)
				}
				time.Sleep(70 * time.Second)
			}

			// wait for all tasks to complete
			time.Sleep(500 * time.Minute)

			tasks := r.List()

			countTerminal := 0
			for _, task := range tasks {
				if task.Status == tempo.TaskStatusComplete || task.Status == tempo.TaskStatusFailed {
					countTerminal++
				}
			}

			want := 15
			if countTerminal != want {
				t.Errorf("unexpected amount of terminal tasks, got: %d, want %d", countTerminal, want)
			}

			err := r.ShutDown(context.Background())
			if err != nil {
				t.Fatalf("unable to shut down server: %v", err)
			}
		})
	})

	t.Run("expect clean with small history", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5, HistorySize: 5})
			r.RegisterRaw("success", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error {
				time.Sleep(1 * time.Minute)
				return nil
			})
			r.RegisterRaw("fail", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error {
				time.Sleep(1 * time.Minute)
				return fmt.Errorf("fail task")
			})
			r.StartBg()

			for i := 1; i <= 10; i++ {
				_, _, err := r.AddRaw("success", nil)
				if err != nil {
					t.Fatal(err)
				}
				time.Sleep(70 * time.Second)
			}
			for i := 1; i <= 5; i++ {
				_, _, err := r.AddRaw("fail", nil)
				if err != nil {
					t.Fatal(err)
				}
				time.Sleep(70 * time.Second)
			}

			// wait for all tasks to complete
			time.Sleep(500 * time.Minute)

			tasks := r.List()

			countTerminal := 0
			for _, task := range tasks {
				if task.Status == tempo.TaskStatusComplete || task.Status == tempo.TaskStatusFailed {
					countTerminal++
				}
			}

			want := 5
			if countTerminal != want {
				t.Errorf("unexpected amount of terminal tasks, got: %d, want %d", countTerminal, want)
			}

			err := r.ShutDown(context.Background())
			if err != nil {
				t.Fatalf("unable to shut down server: %v", err)
			}
		})
	})

}

func TestRunnerShutdown(t *testing.T) {
	t.Run("clean shutdown, wait for waitingTasks to finish", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var result []string
			lock := sync.Mutex{}
			r := newTestRunner(tempo.RunnerCfg{Parallelism: 2, QueueSize: 10})
			r.RegisterRaw("1", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error {
				select {
				case <-time.After(10 * time.Minute):
					lock.Lock()
					result = append(result, "1")
					lock.Unlock()
					return nil
				case <-ctx.Done():
					time.Sleep(1 * time.Minute)
					lock.Lock()
					result = append(result, "1")
					lock.Unlock()
					return nil
				}
			})
			r.RegisterRaw("2", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error {
				select {
				case <-time.After(10 * time.Minute):
					lock.Lock()
					result = append(result, "2")
					lock.Unlock()
					return nil
				case <-ctx.Done():
					time.Sleep(1 * time.Minute)
					lock.Lock()
					result = append(result, "2")
					lock.Unlock()
					return nil
				}
			})
			r.StartBg()

			for i := 1; i <= 2; i++ {
				_, _, err := r.AddRaw(strconv.Itoa(i), nil)
				if err != nil {
					t.Fatal(err)
				}
			}

			time.Sleep(5 * time.Minute)
			err := r.ShutDown(context.Background())
			if err != nil {
				t.Errorf("unable to shut down server: %v", err)
			}

			want := []string{"1", "2"}

			// sort the result, to ensure all jobs were executed
			sort.Slice(result, func(i, j int) bool {
				a, _ := strconv.Atoi(result[i])
				b, _ := strconv.Atoi(result[j])
				return a < b
			})

			if diff := cmp.Diff(result, want); diff != "" {
				t.Errorf("unexpected value (-got +want)\n%s", diff)
			}
		})
	})

	t.Run("unclean shutdown waitingTasks exceed timeout", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var result []string
			lock := sync.Mutex{}
			r := newTestRunner(tempo.RunnerCfg{Parallelism: 2, QueueSize: 10})
			r.RegisterRaw("1", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error {
				select {
				case <-time.After(10 * time.Minute):
					lock.Lock()
					result = append(result, "1")
					lock.Unlock()
					return nil
				case <-ctx.Done():
					time.Sleep(5 * time.Minute)
					lock.Lock()
					result = append(result, "1")
					lock.Unlock()
					return nil
				}
			})
			r.RegisterRaw("2", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error {
				select {
				case <-time.After(10 * time.Minute):
					lock.Lock()
					result = append(result, "2")
					lock.Unlock()
					return nil
				case <-ctx.Done():
					time.Sleep(5 * time.Minute)
					lock.Lock()
					result = append(result, "2")
					lock.Unlock()
					return nil
				}
			})
			r.StartBg()

			for i := 1; i <= 2; i++ {
				_, _, err := r.AddRaw(strconv.Itoa(i), nil)
				if err != nil {
					t.Fatal(err)
				}
			}

			time.Sleep(5 * time.Minute)
			// trigger shutdown before waitingTasks finished,
			// we expect timeout while shutting down waitingTasks

			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()

			err := r.ShutDown(ctx)
			if !errors.Is(err, tempo.ErrUnsafeStop) {
				t.Errorf("expect err to be tempo.ErrUnsafeStop but got %v", err)
			}

			// verify work after shutdown, expect empty result
			var want []string
			lock.Lock()
			if diff := cmp.Diff(result, want); diff != "" {
				t.Errorf("unexpected value (-got +want)\n%s", diff)
			}
			lock.Unlock()

			// white some longer until the shutdown actually exits the routines
			// since in this scenario we expect routines to leak, with the extra waiting time
			// we prevent the bubble goroutine error to trigger
			time.Sleep(10 * time.Minute)
		})
	})
}

func TestRunnerRaceConditions(t *testing.T) {
	t.Run("max parallelism reached", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			r := newTestRunner(tempo.RunnerCfg{Parallelism: 2, QueueSize: 20, HistorySize: 10})
			r.RegisterRaw("some action", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error {
				time.Sleep(10 * time.Minute)
				return nil
			})
			r.StartBg()

			for i := 1; i <= 4; i++ {
				_, _, err := r.AddRaw("some action", nil)
				if err != nil {
					t.Fatal(err)
				}
			}

			// wait to get the tasks scheduled
			time.Sleep(1 * time.Minute)

			gotStatus := getRunnerJobStatus(r.List())
			// important! the want statement is correct, there must be only 2 running tasks
			want := []string{"running", "running", "waiting", "waiting"}
			if diff := cmp.Diff(gotStatus, want); diff != "" {
				t.Errorf("unexpected value (-got +want)\n%s", diff)
			}

			// wait 11 minutes to have the firs 2 jobs finish
			time.Sleep(11 * time.Minute)

			gotStatus = getRunnerJobStatus(r.List())
			want = []string{"complete", "complete", "running", "running"}
			if diff := cmp.Diff(gotStatus, want); diff != "" {
				t.Errorf("unexpected value (-got +want)\n%s", diff)
			}

			go func() {
				// wait before running shutdown, this simulates a signal listener like
				// signal.Notify(make(chan os.Signal, 1), syscall.SIGINT, syscall.SIGTERM)
				time.Sleep(2000 * time.Minute)
				err := r.ShutDown(context.Background())
				if err != nil {
					t.Errorf("unable to shut down server: %v", err)
				}
			}()
			r.Wait()
		})
	})

}

// gatedLogSink holds the worker in the first line it appends for a task, the
// "task started", until release is closed, and signals entered when it gets
// there. It passes every line on to next, if set.
type gatedLogSink struct {
	once    sync.Once
	entered chan struct{}
	release chan struct{}
	next    tempo.TaskLogSink
}

func (s *gatedLogSink) Append(ctx context.Context, id uuid.UUID, level string, msg string) error {
	s.once.Do(func() {
		close(s.entered)
		<-s.release
	})
	if s.next == nil {
		return nil
	}
	return s.next.Append(ctx, id, level, msg)
}

// TestRunnerCancelRacesTaskRegistration guards that Cancel finds a run whose
// worker is held in its first log line, the "task started". The worker once
// recorded the cancelable run-state only after that line, so a slow sink kept
// the task Running in the queue but absent from the runner, and Cancel returned
// "not found in runner" for a task about to execute. This pins that the
// run-state is recorded before the first line; TestRunnerCancelRacingAClaim
// guards that it is recorded as part of the claim itself.
func TestRunnerCancelRacesTaskRegistration(t *testing.T) {
	sink := &gatedLogSink{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5, LogSink: sink})
	r.RegisterRaw("blocked-start", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error {
		<-ctx.Done()
		return ctx.Err()
	})
	r.StartBg()

	id, _, err := r.AddRaw("blocked-start", nil)
	if err != nil {
		t.Fatal(err)
	}

	// The worker has claimed the task (queue status is Running) and is parked in
	// the "task started" log call.
	<-sink.entered

	// The task is present and Running, so Cancel must find it and wait on its
	// run; the worker is still gated, so the short wait ends in a cancel timeout.
	// Reporting the task "not found in runner" was the bug.
	cancelCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	cancelErr := r.Cancel(cancelCtx, id)

	// Let the worker proceed and shut the runner down cleanly, even if the
	// assertion below fails.
	close(sink.release)
	defer func() {
		ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = r.ShutDown(ctx)
	}()

	if !errors.Is(cancelErr, context.DeadlineExceeded) {
		t.Fatalf("Cancel on a claimed task held in its first log line: got %v, want a cancel timeout", cancelErr)
	}
}

// TestRunnerShutDownIsIdempotent guards that a second ShutDown is a no-op and
// that Wait returns after shutdown. stopOnce/stopChan are exactly the kind of
// primitive a refactor can break into a double-close panic.
func TestRunnerShutDownIsIdempotent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5})
		r.RegisterRaw("noop", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error { return nil })
		r.StartBg()

		if err := r.ShutDown(context.Background()); err != nil {
			t.Fatalf("first shutdown: %v", err)
		}
		if err := r.ShutDown(context.Background()); err != nil {
			t.Errorf("a second shutdown should be a no-op, got %v", err)
		}
		r.Wait() // must return promptly once shut down
	})
}

// TestRunnerCancelErrors guards the two error paths of Cancel that no test
// currently covers: an unknown id, and a task that is already terminal.
func TestRunnerCancelErrors(t *testing.T) {
	t.Run("unknown id returns ErrTaskNotFound", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5})
			r.StartBg()
			if err := r.Cancel(context.Background(), uuid.New()); !errors.Is(err, tempo.ErrTaskNotFound) {
				t.Errorf("expected ErrTaskNotFound, got %v", err)
			}
			_ = r.ShutDown(context.Background())
		})
	})

	t.Run("a completed task is not cancelable", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5})
			r.RegisterRaw("quick", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error { return nil })
			r.StartBg()
			id, _, err := r.AddRaw("quick", nil)
			if err != nil {
				t.Fatal(err)
			}
			time.Sleep(1 * time.Minute) // let it complete
			if info, _ := r.GetTask(id); info.Status != tempo.TaskStatusComplete {
				t.Fatalf("precondition: expected task complete, got %s", info.Status.Str())
			}
			err = r.Cancel(context.Background(), id)
			if err == nil || !strings.Contains(err.Error(), "not cancelable") {
				t.Errorf("expected a 'not cancelable' error for a completed task, got %v", err)
			}
			_ = r.ShutDown(context.Background())
		})
	})
}

func TestRunnerCatchPanic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRunner(tempo.RunnerCfg{Parallelism: 2, QueueSize: 20})
		r.RegisterRaw("some action", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error {
			time.Sleep(1 * time.Minute)
			panic("panic")
		})
		r.StartBg()

		for i := 1; i <= 3; i++ {
			_, _, err := r.AddRaw("some action", nil)
			if err != nil {
				t.Fatal(err)
			}
		}

		go func() {
			// wait before running shutdown, this simulates a signal listener like
			// signal.Notify(make(chan os.Signal, 1), syscall.SIGINT, syscall.SIGTERM)
			time.Sleep(2000 * time.Minute)
			err := r.ShutDown(context.Background())
			if err != nil {
				t.Errorf("unable to shut down server: %v", err)
			}
		}()
		r.Wait()
	})
}

func getRunnerJobStatus(in []tempo.TaskInfo) []string {
	r := []string{}
	for _, item := range in {
		r = append(r, item.Status.Str())
	}
	return r
}

//nolint:gocyclo // test should be easy to read
func TestRunnerCancel(t *testing.T) {
	t.Run("cancel waiting task", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			fn := func(ctx context.Context) error {
				timer := time.NewTimer(time.Hour * 10)
				defer timer.Stop()
				select {
				case <-ctx.Done():
					return nil
				case <-timer.C:
					return nil
				}
			}
			r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 20})
			r.RegisterRaw("running task", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error { return fn(ctx) })
			r.RegisterRaw("waiting task", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error { return fn(ctx) })
			r.StartBg()

			_, _, err := r.AddRaw("running task", nil)
			if err != nil {
				t.Fatal(err)
			}

			id, _, err := r.AddRaw("waiting task", nil)
			if err != nil {
				t.Fatal(err)
			}

			time.Sleep(3 * time.Minute)

			err = r.Cancel(t.Context(), id)
			if err != nil {
				t.Fatalf("unable to cancel job: %v", err)
			}

			info, err := r.GetTask(id)
			if err != nil {
				t.Fatalf("unable to get task: %v", err)
			}
			if info.Status != tempo.TaskStatusCanceled {
				t.Errorf("expecting task to be in status %s but got %s", tempo.TaskStatusCanceled.Str(), info.Status.Str())
			}

			if info.EndedAt.IsZero() {
				t.Errorf("task end date should not be zero")
			}

			go func() {
				// wait before running shutdown, this simulates a signal listener like
				// signal.Notify(make(chan os.Signal, 1), syscall.SIGINT, syscall.SIGTERM)
				time.Sleep(2000 * time.Minute)
				err := r.ShutDown(context.Background())
				if err != nil {
					t.Errorf("unable to shut down server: %v", err)
				}
			}()
			r.Wait()
		})
	})

	t.Run("clean stop of running task", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			msgs := []string{}
			lock := sync.Mutex{}
			fn := func(ctx context.Context) error {
				ticker := time.NewTicker(time.Minute)
				defer ticker.Stop()
				i := 1
				for {
					select {
					case <-ctx.Done():
						lock.Lock()
						msgs = append(msgs, "clean shutdown")
						lock.Unlock()
						return ctx.Err() // return Canceled so runner records TaskStatusCanceled
					case <-ticker.C:
						lock.Lock()
						msgs = append(msgs, fmt.Sprintf("msg %d", i))
						lock.Unlock()
						i++
					}
				}
			}
			r := newTestRunner(tempo.RunnerCfg{Parallelism: 2, QueueSize: 20})
			r.RegisterRaw("timeout_fn", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error { return fn(ctx) })
			r.StartBg()

			id, _, err := r.AddRaw("timeout_fn", nil)
			if err != nil {
				t.Fatal(err)
			}

			time.Sleep(3 * time.Minute)

			err = r.Cancel(t.Context(), id)
			if err != nil {
				t.Fatalf("unable to cancel job: %v", err)
			}

			info, err := r.GetTask(id)
			if err != nil {
				t.Fatalf("unable to get task: %v", err)
			}
			if info.Status != tempo.TaskStatusCanceled {
				t.Errorf("expecting task to be in status %s but got %s", tempo.TaskStatusCanceled.Str(), info.Status.Str())
			}
			if info.EndedAt.IsZero() {
				t.Errorf("task end date should not be zero")
			}

			go func() {
				// wait before running shutdown, this simulates a signal listener like
				// signal.Notify(make(chan os.Signal, 1), syscall.SIGINT, syscall.SIGTERM)
				time.Sleep(2000 * time.Minute)
				err := r.ShutDown(context.Background())
				if err != nil {
					t.Errorf("unable to shut down server: %v", err)
				}
			}()
			r.Wait()

			want := []string{
				"msg 1", "msg 2", "msg 3", "clean shutdown",
			}
			lock.Lock()
			got := msgs
			lock.Unlock()
			if diff := cmp.Diff(got, want); diff != "" {
				t.Errorf("unexpected value (-got +want)\n%s", diff)
			}
		})
	})

	t.Run("cancel task error", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			msgs := []string{}
			lock := sync.Mutex{}
			taskDone := make(chan any, 1)
			fn := func(ctx context.Context) error {
				ticker := time.NewTicker(time.Minute)
				defer ticker.Stop()
				i := 1
				for {
					select {
					case <-ctx.Done():
						lock.Lock()
						msgs = append(msgs, "received shutdown, but ignoring it")
						lock.Unlock()
						<-taskDone
						return nil
					case <-ticker.C:
						lock.Lock()
						msgs = append(msgs, fmt.Sprintf("msg %d", i))
						lock.Unlock()
						i++
					}
				}
			}
			r := newTestRunner(tempo.RunnerCfg{Parallelism: 2, QueueSize: 20})
			r.RegisterRaw("timeout_fn", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error { return fn(ctx) })
			r.StartBg()

			id, _, err := r.AddRaw("timeout_fn", nil)
			if err != nil {
				t.Fatal(err)
			}

			time.Sleep(3 * time.Minute)

			timeoutCtx, cfn := context.WithTimeout(t.Context(), 5*time.Second)
			defer cfn()

			err = r.Cancel(timeoutCtx, id)
			if err == nil {
				t.Errorf("expecting cancel error")
			}

			info, err := r.GetTask(id)
			if err != nil {
				t.Fatalf("unable to get task: %v", err)
			}
			if info.Status != tempo.TaskStatusCancelError {
				t.Errorf("expecting task to be in status %s but got %s", tempo.TaskStatusCancelError.Str(), info.Status.Str())
			}
			if info.EndedAt.IsZero() {
				t.Errorf("task end date should not be zero")
			}

			go func() {
				// wait before running shutdown, this simulates a signal listener like
				// signal.Notify(make(chan os.Signal, 1), syscall.SIGINT, syscall.SIGTERM)
				time.Sleep(2000 * time.Minute)

				shutdownTimeout, cfn := context.WithTimeout(t.Context(), 5*time.Second)
				defer cfn()
				err := r.ShutDown(shutdownTimeout)
				if err == nil {
					t.Errorf("expecting shutdown timeout error")
				}
			}()
			r.Wait()

			want := []string{
				"msg 1", "msg 2", "msg 3", "received shutdown, but ignoring it",
			}
			lock.Lock()
			got := msgs
			lock.Unlock()
			if diff := cmp.Diff(got, want); diff != "" {
				t.Errorf("unexpected value (-got +want)\n%s", diff)
			}
			// close the hung routine before exiting the test
			taskDone <- struct{}{}

		})
	})
}

// TestRunnerCancelRecordsTaskResult guards that a canceled running task ends
// with the status its own return value gives, the rule ShutDown follows too,
// and that the status is in place by the time Cancel returns. Cancel used to
// write Canceled after the run while the worker wrote the task's result, so the
// last write won: a task returning nil ended complete or canceled by timing.
func TestRunnerCancelRecordsTaskResult(t *testing.T) {
	tcs := []struct {
		name string
		stop func(ctx context.Context) error
		want tempo.TaskStatus
	}{
		{name: "returns ctx.Err()", stop: func(ctx context.Context) error { return ctx.Err() }, want: tempo.TaskStatusCanceled},
		{name: "returns nil", stop: func(context.Context) error { return nil }, want: tempo.TaskStatusComplete},
		{name: "returns an error", stop: func(context.Context) error { return errors.New("cleanup failed") }, want: tempo.TaskStatusFailed},
		{name: "panics", stop: func(context.Context) error { panic("cleanup panicked") }, want: tempo.TaskStatusPanicked},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5})
				r.RegisterRaw("task", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error {
					<-ctx.Done()
					return tc.stop(ctx)
				})
				r.StartBg()
				id, _, err := r.AddRaw("task", nil)
				if err != nil {
					t.Fatal(err)
				}
				synctest.Wait() // the task is running, parked on ctx.Done

				if err := r.Cancel(context.Background(), id); err != nil {
					t.Fatalf("Cancel: %v", err)
				}
				if info, _ := r.GetTask(id); info.Status != tc.want {
					t.Errorf("status when Cancel returned: got %s, want %s", info.Status.Str(), tc.want.Str())
				}

				if err := r.ShutDown(context.Background()); err != nil {
					t.Fatalf("ShutDown: %v", err)
				}
				if info, _ := r.GetTask(id); info.Status != tc.want {
					t.Errorf("status once the runner stopped: got %s, want %s", info.Status.Str(), tc.want.Str())
				}
			})
		})
	}
}

// gatedFinalStatusPersistence parks SaveTask for a task's final status until
// release is closed, the way a slow store would hold the worker there.
type gatedFinalStatusPersistence struct {
	*tempo.MemPersistence
	release chan struct{}
}

func (p gatedFinalStatusPersistence) SaveTask(ctx context.Context, task tempo.TaskInfo) error {
	if slices.Contains(tempo.TaskTerminalStatus, task.Status) {
		<-p.release
	}
	return p.MemPersistence.SaveTask(ctx, task)
}

// TestRunnerCancelReturnsOnceFinalStatusIsRecorded guards that Cancel returns
// only after the worker has recorded the run's final status, store included, so
// a caller reading the task right after Cancel sees how it ended. The worker
// records it before it signals the run done; in the reverse order, Cancel
// returned while the task still read Running.
func TestRunnerCancelReturnsOnceFinalStatusIsRecorded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		persist := gatedFinalStatusPersistence{MemPersistence: tempo.NewMemPersistence(), release: make(chan struct{})}
		r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5, Persistence: persist})
		r.RegisterRaw("task", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error {
			<-ctx.Done()
			return ctx.Err()
		})
		r.StartBg()
		id, _, err := r.AddRaw("task", nil)
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait() // the task is running, parked on ctx.Done

		var cancelErr error
		returned := make(chan struct{})
		go func() {
			cancelErr = r.Cancel(context.Background(), id)
			close(returned)
		}()
		synctest.Wait() // the worker is held in recording the final status
		select {
		case <-returned:
			t.Error("Cancel returned before the run's final status was recorded")
		default:
		}

		close(persist.release)
		<-returned
		if cancelErr != nil {
			t.Fatalf("Cancel: %v", cancelErr)
		}
		if info, _ := r.GetTask(id); info.Status != tempo.TaskStatusCanceled {
			t.Errorf("status when Cancel returned: got %s, want canceled", info.Status.Str())
		}
		if err := r.ShutDown(context.Background()); err != nil {
			t.Fatalf("ShutDown: %v", err)
		}
	})
}

// TestRunnerCancelTimeoutKeepsFinalStatus guards that a Cancel giving up after
// the run stopped never leaves it in cancel_error. That mark is for a run still
// going, so it must not land over the status the worker recorded: cancel_error
// is not terminal, and CleanHistory would keep the task forever. The store holds
// the worker right after it records canceled, and Cancel gives up only then.
func TestRunnerCancelTimeoutKeepsFinalStatus(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		persist := gatedFinalStatusPersistence{MemPersistence: tempo.NewMemPersistence(), release: make(chan struct{})}
		r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5, Persistence: persist})
		r.RegisterRaw("task", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error {
			<-ctx.Done()
			return ctx.Err()
		})
		r.StartBg()
		id, _, err := r.AddRaw("task", nil)
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait() // the task is running, parked on ctx.Done

		ctx, giveUp := context.WithCancel(context.Background())
		returned := make(chan error, 1)
		go func() { returned <- r.Cancel(ctx, id) }()
		synctest.Wait() // the worker has recorded canceled and is held storing it
		giveUp()        // so Cancel stops waiting only after that write
		close(persist.release)
		if err := <-returned; !errors.Is(err, context.Canceled) {
			t.Fatalf("Cancel: got %v, want it to give up", err)
		}
		if err := r.ShutDown(context.Background()); err != nil {
			t.Fatalf("ShutDown: %v", err)
		}
		if info, _ := r.GetTask(id); info.Status != tempo.TaskStatusCanceled {
			t.Errorf("status: got %s, want canceled", info.Status.Str())
		}
	})
}

// errorLineGatedLogSink parks the worker inside the first ERROR line it appends
// for a task (a panic's message) until release is closed, and signals entered
// when it gets there.
type errorLineGatedLogSink struct {
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (s *errorLineGatedLogSink) Append(_ context.Context, _ uuid.UUID, level string, _ string) error {
	if level != "ERROR" {
		return nil
	}
	s.once.Do(func() {
		close(s.entered)
		<-s.release
	})
	return nil
}

// TestRunnerCancelFindsAFinishingRun guards that a run stays cancelable until
// its final status is recorded. The worker used to drop the run-state before it
// logged a panic and recorded Panicked, so a Cancel in between saw the task
// Running in the queue, absent from the runner, and returned "not found in
// runner". The sink holds the worker in the panic's log line.
func TestRunnerCancelFindsAFinishingRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sink := &errorLineGatedLogSink{entered: make(chan struct{}), release: make(chan struct{})}
		r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5, LogSink: sink})
		r.RegisterRaw("panics", func(context.Context, *slog.Logger, tempo.Progress, []byte) error {
			panic("boom")
		})
		r.StartBg()
		id, _, err := r.AddRaw("panics", nil)
		if err != nil {
			t.Fatal(err)
		}
		<-sink.entered

		// The worker is held in the log call, so Cancel can only wait for it: a
		// timeout is the expected result, a task reported absent is the bug.
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := r.Cancel(ctx, id); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Cancel on a run that is still finishing: got %v, want a cancel timeout", err)
		}

		close(sink.release)
		if err := r.ShutDown(context.Background()); err != nil {
			t.Fatalf("ShutDown: %v", err)
		}
		if info, _ := r.GetTask(id); info.Status != tempo.TaskStatusPanicked {
			t.Errorf("status once the run finished: got %s, want panicked", info.Status.Str())
		}
	})
}

// TestRunnerCancelRacingAClaim guards Cancel against a worker claiming the same
// task. Cancel used to read Waiting and write Canceled in two steps, so a claim
// in between left a live run marked canceled while Cancel reported success; and
// a claim just before it showed the task Running before the worker registered
// the run, so Cancel returned "not found in runner". Both windows are narrow,
// so the test cancels many tasks the moment they are enqueued while several
// workers claim them.
func TestRunnerCancelRacingAClaim(t *testing.T) {
	const workers, cancelers, perCanceler = 8, 8, 500
	// A canceler adds again only once its Cancel returned, so at most one task
	// per canceler is ever waiting: a QueueSize of cancelers never fills.
	r := newTestRunner(tempo.RunnerCfg{Parallelism: workers, QueueSize: cancelers})
	var live atomic.Int64 // runs started and not yet returned
	r.RegisterRaw("blocks-until-canceled", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error {
		live.Add(1)
		defer live.Add(-1)
		<-ctx.Done()
		return ctx.Err()
	})
	r.StartBg()

	var failed atomic.Int64
	var wg sync.WaitGroup
	for range cancelers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perCanceler {
				id, _, err := r.AddRaw("blocks-until-canceled", nil)
				if err != nil {
					t.Error(err)
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err = r.Cancel(ctx, id)
				cancel()
				if err != nil && failed.Add(1) <= 3 {
					t.Errorf("Cancel of a just-enqueued task: %v", err)
				}
			}
		}()
	}
	wg.Wait()

	if n := failed.Load(); n > 0 {
		t.Errorf("%d of %d Cancel calls failed", n, cancelers*perCanceler)
	}
	// A task canceled while waiting never starts, and Cancel returns only once a
	// running one has stopped: with every Cancel returned, no run may be left.
	if n := live.Load(); n != 0 {
		t.Errorf("%d runs still going after every Cancel returned", n)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.ShutDown(ctx); err != nil {
		t.Fatalf("ShutDown: %v", err)
	}
	// Each task was canceled waiting or running, so each ends canceled: none is
	// left running or in cancel_error.
	var notCanceled int
	for _, info := range r.List() {
		if info.Status != tempo.TaskStatusCanceled {
			notCanceled++
		}
	}
	if notCanceled > 0 {
		t.Errorf("%d tasks not canceled once the runner stopped", notCanceled)
	}
}

// TestRunnerFailsAnUnregisteredTask guards the path for a claimed task whose
// name has no handler (AddRaw does not check, and a restart can load one): it
// ends failed and leaves no run behind, so Cancel reports it not cancelable
// instead of waiting on a run that never finishes.
func TestRunnerFailsAnUnregisteredTask(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5})
		r.StartBg()
		id, _, err := r.AddRaw("never-registered", nil)
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait()

		if info, _ := r.GetTask(id); info.Status != tempo.TaskStatusFailed {
			t.Errorf("status: got %s, want failed", info.Status.Str())
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := r.Cancel(ctx, id); err == nil || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, tempo.ErrTaskNotFound) {
			t.Errorf("Cancel on a failed task: got %v, want a not-cancelable error", err)
		}
		if err := r.ShutDown(context.Background()); err != nil {
			t.Fatalf("ShutDown: %v", err)
		}
	})
}

// TestRunnerFailsATaskThatExitsItsGoroutine guards a task that ends its goroutine
// instead of returning, as t.FailNow does when a test's task calls it: it ends
// failed and frees its slot. The worker retired a run only once its task
// returned, so such a task stayed running and held its exclusion group, and a
// Cancel waited on it until its own ctx ran out.
func TestRunnerFailsATaskThatExitsItsGoroutine(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Two workers: the one running "exits" ends with it.
		r := newTestRunner(tempo.RunnerCfg{Parallelism: 2, QueueSize: 5})
		r.RegisterRaw("exits", func(context.Context, *slog.Logger, tempo.Progress, []byte) error {
			runtime.Goexit()
			return nil
		}, tempo.WithExclusionGroup("files"))
		r.RegisterRaw("next", func(context.Context, *slog.Logger, tempo.Progress, []byte) error {
			return nil
		}, tempo.WithExclusionGroup("files"))
		r.StartBg()

		exits, _, err := r.AddRaw("exits", nil)
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		next, _, err := r.AddRaw("next", nil)
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait()

		if info, _ := r.GetTask(exits); info.Status != tempo.TaskStatusFailed {
			t.Errorf("task that exited its goroutine: got %s, want failed", info.Status.Str())
		}
		if info, _ := r.GetTask(next); info.Status != tempo.TaskStatusComplete {
			t.Errorf("next task in its exclusion group: got %s, want complete", info.Status.Str())
		}
		if err := r.ShutDown(context.Background()); err != nil {
			t.Fatalf("ShutDown: %v", err)
		}
	})
}

// ctxAwareStore fails a write whose ctx is already done, the way a store backed
// by a ctx-aware client (dbqueue's gorm one) does.
type ctxAwareStore struct {
	*recoverableMem
}

func (s ctxAwareStore) SaveTask(ctx context.Context, task tempo.TaskInfo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.recoverableMem.SaveTask(ctx, task)
}

// TestRunnerCancelStoresAWaitingTaskCanceled guards that canceling a waiting
// task reaches the store even when Cancel's ctx is already done, as when the
// request that asked for it went away. The write used to go out on that ctx, so
// a store honouring it failed it: the queue said canceled, the store still said
// waiting, and after a restart the canceled task ran.
func TestRunnerCancelStoresAWaitingTaskCanceled(t *testing.T) {
	store := ctxAwareStore{newRecoverableMem()}
	// Neither runner is started, so the task stays waiting until canceled.
	r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5, Persistence: store})
	id, _, err := r.AddRaw("task", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Cancel(ctx, id); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	restarted := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5, Persistence: store})
	info, err := restarted.GetTask(id)
	if err != nil {
		t.Fatalf("GetTask after a restart: %v", err)
	}
	if info.Status != tempo.TaskStatusCanceled {
		t.Errorf("status after a restart: got %s, want canceled", info.Status.Str())
	}
}

func waitTerminal(t *testing.T, r *tempo.QueueRunner, id uuid.UUID) tempo.TaskInfo {
	t.Helper()
	for i := 0; i < 400; i++ {
		info, err := r.GetTask(id)
		if err == nil && slices.Contains(tempo.TaskTerminalStatus, info.Status) {
			return info
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("task %s did not reach a terminal status", id)
	return tempo.TaskInfo{}
}

func TestTaskLoggerRendersAttrs(t *testing.T) {
	ctx := context.Background()
	sink := tempo.NewMemTaskLogSink()
	r, err := tempo.NewQueueRunner(tempo.RunnerCfg{
		Parallelism: 1, QueueSize: 10, Persistence: tempo.NewMemPersistence(),
		LogSink: sink, LogLevel: slog.LevelInfo,
	})
	if err != nil {
		t.Fatal(err)
	}
	r.RegisterRaw("t", func(_ context.Context, log *slog.Logger, _ tempo.Progress, _ []byte) error {
		log.With("job", "x").Info("done", "count", 5)
		return nil
	})
	r.StartBg()
	id, _, err := r.AddRaw("t", nil)
	if err != nil {
		t.Fatal(err)
	}
	waitTerminal(t, r, id)
	sctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_ = r.ShutDown(sctx)

	entries, _ := sink.Logs(ctx, id)
	var found bool
	for _, e := range entries {
		if e.Message == "done job=x count=5" {
			found = true
		}
	}
	if !found {
		t.Fatalf("rendered attr line not found in %+v", entries)
	}
}

func TestLifecycleLoggedAboveJobLevel(t *testing.T) {
	ctx := context.Background()
	sink := tempo.NewMemTaskLogSink()
	r, err := tempo.NewQueueRunner(tempo.RunnerCfg{
		Parallelism: 1, QueueSize: 10, Persistence: tempo.NewMemPersistence(),
		LogSink: sink, LogLevel: slog.LevelError, // job-level lines suppressed
	})
	if err != nil {
		t.Fatal(err)
	}
	r.RegisterRaw("t", func(_ context.Context, log *slog.Logger, _ tempo.Progress, _ []byte) error {
		log.Info("suppressed") // below LogLevel -> dropped
		return nil
	})
	r.StartBg()
	id, _, _ := r.AddRaw("t", nil)
	waitTerminal(t, r, id)
	sctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_ = r.ShutDown(sctx)

	entries, _ := sink.Logs(ctx, id)
	var started, suppressed bool
	for _, e := range entries {
		switch e.Message {
		case "task started", "task finished":
			started = true
		case "suppressed":
			suppressed = true
		}
	}
	if !started {
		t.Fatal("lifecycle line missing despite high LogLevel")
	}
	if suppressed {
		t.Fatal("job line below LogLevel should have been dropped")
	}
}

// ctxAwareLogSink drops a line whose ctx is already done, the way a sink backed
// by a ctx-aware client (a gorm one, say) fails its write on a canceled ctx.
type ctxAwareLogSink struct {
	*tempo.MemTaskLogSink
}

func (s ctxAwareLogSink) Append(ctx context.Context, id uuid.UUID, level string, msg string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.MemTaskLogSink.Append(ctx, id, level, msg)
}

// TestRunnerLogsACanceledTaskLastLine guards that the line the runner writes as
// a canceled task returns reaches a sink that honours ctx. It used to be
// appended on the task's own ctx, canceled by then, so such a sink dropped it
// and the task's log ended without saying how the run ended.
func TestRunnerLogsACanceledTaskLastLine(t *testing.T) {
	tcs := []struct {
		name      string
		stop      func(ctx context.Context) error
		wantLevel string
		wantMsg   string
	}{
		{name: "returns ctx.Err()", stop: func(ctx context.Context) error { return ctx.Err() }, wantLevel: "INFO", wantMsg: "task canceled"},
		{name: "returns nil", stop: func(context.Context) error { return nil }, wantLevel: "INFO", wantMsg: "task finished"},
		{name: "returns an error", stop: func(context.Context) error { return errors.New("cleanup failed") }, wantLevel: "ERROR", wantMsg: "cleanup failed"},
		{name: "panics", stop: func(context.Context) error { panic("cleanup panicked") }, wantLevel: "ERROR", wantMsg: "cleanup panicked"},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sink := ctxAwareLogSink{tempo.NewMemTaskLogSink()}
				r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5, LogSink: sink})
				r.RegisterRaw("task", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error {
					<-ctx.Done()
					return tc.stop(ctx)
				})
				r.StartBg()
				id, _, err := r.AddRaw("task", nil)
				if err != nil {
					t.Fatal(err)
				}
				synctest.Wait() // the task is running, parked on ctx.Done

				if err := r.Cancel(context.Background(), id); err != nil {
					t.Fatalf("Cancel: %v", err)
				}
				if err := r.ShutDown(context.Background()); err != nil {
					t.Fatalf("ShutDown: %v", err)
				}

				entries, _ := sink.Logs(context.Background(), id)
				if len(entries) == 0 {
					t.Fatal("no log lines recorded")
				}
				if last := entries[len(entries)-1]; last.Level != tc.wantLevel || last.Message != tc.wantMsg {
					t.Errorf("last line: got %s %q, want %s %q", last.Level, last.Message, tc.wantLevel, tc.wantMsg)
				}
			})
		})
	}
}

// TestRunnerLogsACanceledTaskFirstLine guards that a run canceled before its
// "task started" line is written still logs that line on a sink that honours
// ctx. A run can be canceled from its claim on; the line went out on the task's
// own ctx, so such a sink dropped it and the log began with how the run ended.
func TestRunnerLogsACanceledTaskFirstLine(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mem := tempo.NewMemTaskLogSink()
		sink := &gatedLogSink{entered: make(chan struct{}), release: make(chan struct{}), next: ctxAwareLogSink{mem}}
		r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5, LogSink: sink})
		r.RegisterRaw("task", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error {
			<-ctx.Done()
			return ctx.Err()
		})
		r.StartBg()
		id, _, err := r.AddRaw("task", nil)
		if err != nil {
			t.Fatal(err)
		}
		<-sink.entered // the worker holds the "task started" line

		// Cancel cancels the run's ctx, then gives up waiting on the held worker.
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := r.Cancel(ctx, id); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Cancel on a run held in its first line: got %v, want a cancel timeout", err)
		}
		close(sink.release)
		if err := r.ShutDown(context.Background()); err != nil {
			t.Fatalf("ShutDown: %v", err)
		}

		entries, _ := mem.Logs(context.Background(), id)
		var got []string
		for _, e := range entries {
			got = append(got, e.Message)
		}
		if want := []string{"task started", "task canceled"}; !slices.Equal(got, want) {
			t.Errorf("log lines: got %q, want %q", got, want)
		}
	})
}

func TestTaskReporterReachesSink(t *testing.T) {
	ctx := context.Background()
	psink := tempo.NewMemTaskProgressSink()
	r, err := tempo.NewQueueRunner(tempo.RunnerCfg{
		Parallelism: 1, QueueSize: 10, Persistence: tempo.NewMemPersistence(),
		ProgressSink: psink,
	})
	if err != nil {
		t.Fatal(err)
	}
	r.RegisterRaw("t", func(_ context.Context, _ *slog.Logger, prog tempo.Progress, _ []byte) error {
		prog.SetTotal(3)
		prog.SetStage("working")
		for i := 0; i < 3; i++ {
			prog.Inc(1)
		}
		return nil
	})
	r.StartBg()
	id, _, err := r.AddRaw("t", nil)
	if err != nil {
		t.Fatal(err)
	}
	info := waitTerminal(t, r, id)
	sctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_ = r.ShutDown(sctx)

	got, ok, _ := psink.Progress(ctx, id)
	if !ok || got.Done != 3 || got.Total != 3 || got.Stage != "working" {
		t.Fatalf("Progress = %+v, ok=%v; want Done=3 Total=3 Stage=working", got, ok)
	}
	if pct, _ := got.Percent(); pct != 1 {
		t.Fatalf("Percent = %v; want 1", pct)
	}
	if eta, ok := got.ETA(info.StartedAt); !ok || eta != 0 {
		t.Fatalf("ETA(info.StartedAt) = %v, %v; want 0, true (task complete)", eta, ok)
	}
}

// test throw error if task is added before the shceduler was stareted
// get job staus after completion => ok, error, panic
