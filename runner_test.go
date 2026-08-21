package tempo_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
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
		r.RegisterRaw("some action", func(ctx context.Context, _ []byte) error {
			time.Sleep(10 * time.Minute)
			return nil
		})
		r.StartBg()

		_, err := r.AddRaw("some action", nil)
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
				r.RegisterRaw(name, func(ctx context.Context, _ []byte) error {
					lock.Lock()
					result = append(result, strconv.Itoa(n))
					lock.Unlock()
					time.Sleep(10 * time.Minute)
					return nil
				})
			}
			r.StartBg()

			for i := 1; i <= 4; i++ {
				_, err := r.AddRaw(strconv.Itoa(i), nil)
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
				r.RegisterRaw(name, func(ctx context.Context, _ []byte) error {
					lock.Lock()
					result = append(result, strconv.Itoa(n))
					lock.Unlock()
					time.Sleep(10 * time.Minute)
					return nil
				})
			}
			r.StartBg()

			for i := 1; i <= 12; i++ {
				_, err := r.AddRaw(strconv.Itoa(i), nil)
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
		r.RegisterRaw("some action", func(ctx context.Context, _ []byte) error {
			time.Sleep(10 * time.Minute)
			return nil
		})
		r.StartBg()

		_, err := r.AddRaw("some action", nil)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)

		for i := 1; i <= 5; i++ {
			_, err := r.AddRaw("some action", nil)
			if err != nil {
				t.Fatal(err)
			}
		}

		_, err = r.AddRaw("some action", nil)

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
			r.RegisterRaw("success", func(ctx context.Context, _ []byte) error { time.Sleep(1 * time.Minute); return nil })
			r.RegisterRaw("fail", func(ctx context.Context, _ []byte) error { time.Sleep(1 * time.Minute); return fmt.Errorf("fail task") })
			r.StartBg()

			for i := 1; i <= 10; i++ {
				_, err := r.AddRaw("success", nil)
				if err != nil {
					t.Fatal(err)
				}
				time.Sleep(70 * time.Second)
			}
			for i := 1; i <= 5; i++ {
				_, err := r.AddRaw("fail", nil)
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
			r.RegisterRaw("success", func(ctx context.Context, _ []byte) error { time.Sleep(1 * time.Minute); return nil })
			r.RegisterRaw("fail", func(ctx context.Context, _ []byte) error { time.Sleep(1 * time.Minute); return fmt.Errorf("fail task") })
			r.StartBg()

			for i := 1; i <= 10; i++ {
				_, err := r.AddRaw("success", nil)
				if err != nil {
					t.Fatal(err)
				}
				time.Sleep(70 * time.Second)
			}
			for i := 1; i <= 5; i++ {
				_, err := r.AddRaw("fail", nil)
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
			r.RegisterRaw("1", func(ctx context.Context, _ []byte) error {
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
			r.RegisterRaw("2", func(ctx context.Context, _ []byte) error {
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
				_, err := r.AddRaw(strconv.Itoa(i), nil)
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
			r.RegisterRaw("1", func(ctx context.Context, _ []byte) error {
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
			r.RegisterRaw("2", func(ctx context.Context, _ []byte) error {
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
				_, err := r.AddRaw(strconv.Itoa(i), nil)
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
			r.RegisterRaw("some action", func(ctx context.Context, _ []byte) error {
				time.Sleep(10 * time.Minute)
				return nil
			})
			r.StartBg()

			for i := 1; i <= 4; i++ {
				_, err := r.AddRaw("some action", nil)
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

// gatedLogSink blocks the worker inside the very first log call it makes for a
// task — the "task started" line — until release is closed, and signals entered
// when it gets there. It lets a test freeze a worker in the window between
// claiming a task and recording its run-state.
type gatedLogSink struct {
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (s *gatedLogSink) Append(_ context.Context, _ uuid.UUID, _ string, _ string) error {
	s.once.Do(func() {
		close(s.entered)
		<-s.release
	})
	return nil
}

// TestRunnerCancelRacesTaskRegistration exposes a race between Cancel and a
// worker that has just claimed a task but has not yet recorded its run-state.
//
// A worker marks a task Running (in the queue) inside NextTask, then only a few
// statements later records the cancelable run-state in r.running. Cancel looks
// the task up in r.running first; not finding it there, it falls back to the
// queue, sees Running, and returns "task ... not found in runner" — refusing to
// cancel a task that is genuinely present and about to execute, and leaving it
// uncanceled.
//
// The window is normally tiny, but the "task started" log line the worker emits
// (when a LogSink is configured) sits inside it, so a slow sink holds the worker
// there deterministically. LogSink is a real feature, so this is an ordering a
// production sink can genuinely expose.
//
// EXPECTED TO FAIL until the run-state is recorded no later than the task
// becomes observable as Running.
func TestRunnerCancelRacesTaskRegistration(t *testing.T) {
	sink := &gatedLogSink{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5, LogSink: sink})
	r.RegisterRaw("blocked-start", func(ctx context.Context, _ []byte) error {
		<-ctx.Done()
		return ctx.Err()
	})
	r.StartBg()

	id, err := r.AddRaw("blocked-start", nil)
	if err != nil {
		t.Fatal(err)
	}

	// The worker has claimed the task (queue status is Running) and is parked in
	// the "task started" log call, before it recorded the run-state.
	<-sink.entered

	// The task is present and Running, so this cancel must not report it absent.
	// A short timeout keeps the call bounded: a fix that finds the task and waits
	// on its done channel returns a cancel-timeout here (the worker is still
	// gated), which is fine — what must not happen is "not found in runner".
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

	if cancelErr != nil && strings.Contains(cancelErr.Error(), "not found in runner") {
		t.Fatalf("Cancel refused a claimed, running task it could not see in r.running: %v", cancelErr)
	}
}

// TestRunnerShutDownIsIdempotent guards that a second ShutDown is a no-op and
// that Wait returns after shutdown. stopOnce/stopChan are exactly the kind of
// primitive a refactor can break into a double-close panic.
func TestRunnerShutDownIsIdempotent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5})
		r.RegisterRaw("noop", func(ctx context.Context, _ []byte) error { return nil })
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
			r.RegisterRaw("quick", func(ctx context.Context, _ []byte) error { return nil })
			r.StartBg()
			id, err := r.AddRaw("quick", nil)
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
		r.RegisterRaw("some action", func(ctx context.Context, _ []byte) error {
			time.Sleep(1 * time.Minute)
			panic("panic")
		})
		r.StartBg()

		for i := 1; i <= 3; i++ {
			_, err := r.AddRaw("some action", nil)
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
			r.RegisterRaw("running task", func(ctx context.Context, _ []byte) error { return fn(ctx) })
			r.RegisterRaw("waiting task", func(ctx context.Context, _ []byte) error { return fn(ctx) })
			r.StartBg()

			_, err := r.AddRaw("running task", nil)
			if err != nil {
				t.Fatal(err)
			}

			id, err := r.AddRaw("waiting task", nil)
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
			r.RegisterRaw("timeout_fn", func(ctx context.Context, _ []byte) error { return fn(ctx) })
			r.StartBg()

			id, err := r.AddRaw("timeout_fn", nil)
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
			r.RegisterRaw("timeout_fn", func(ctx context.Context, _ []byte) error { return fn(ctx) })
			r.StartBg()

			id, err := r.AddRaw("timeout_fn", nil)
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

// test throw error if task is added before the shceduler was stareted
// get job staus after completion => ok, error, panic
