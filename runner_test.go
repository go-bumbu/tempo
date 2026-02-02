package tempo_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-bumbu/tempo"
	"github.com/google/go-cmp/cmp"
	"sort"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestRunnerParallelism(t *testing.T) {

	t.Run("run waitingTasks sequentially with parallelism 1", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			r := tempo.NewQueueRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 10})
			r.StartBg()

			var result []string
			lock := sync.Mutex{}
			for i := 1; i <= 4; i++ {
				n := i
				_, err := r.Add(func(ctx context.Context) error {
					lock.Lock()
					result = append(result, strconv.Itoa(n))
					lock.Unlock()
					time.Sleep(10 * time.Minute)
					return nil
				}, strconv.Itoa(n))
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
			r := tempo.NewQueueRunner(tempo.RunnerCfg{Parallelism: 3, QueueSize: 20})
			r.StartBg()

			var result []string
			lock := sync.Mutex{}
			for i := 1; i <= 12; i++ {
				_, err := r.Add(func(ctx context.Context) error {
					lock.Lock()
					result = append(result, strconv.Itoa(i))
					lock.Unlock()
					time.Sleep(10 * time.Minute)
					return nil
				}, "some action")
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
		r := tempo.NewQueueRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5})
		r.StartBg()

		// put one task into running
		_, err := r.Add(func(ctx context.Context) error {
			time.Sleep(10 * time.Minute)
			return nil
		}, "some action")
		if err != nil {
			t.Fatal(err)
		}
		// let it schedule
		time.Sleep(100 * time.Millisecond)

		// fill TaskQueue up to capacity
		for i := 1; i <= 5; i++ {
			_, err := r.Add(func(ctx context.Context) error {
				time.Sleep(10 * time.Minute)
				return nil
			}, "some action")
			if err != nil {
				t.Fatal(err)
			}
		}

		// expect que full error
		_, err = r.Add(func(ctx context.Context) error {
			time.Sleep(10 * time.Minute)
			return nil
		}, "some action")

		if !errors.Is(err, tempo.ErrQueueFull) {
			t.Errorf("expect err to be tempo.ErrQueueFull but got %v", err)
		}

		go func() {
			// wait before running shutdown, this simulates a signal listener like
			// signal.Notify(make(chan os.Signal, 1), syscall.SIGINT, syscall.SIGTERM)
			time.Sleep(2000 * time.Minute)
			err = r.ShutDown(context.Background())
			if err != nil {
				t.Errorf("unable to shut down server: %v", err)
			}
		}()
		r.Wait()
	})
}

func TestRunnerShutdown(t *testing.T) {
	t.Run("clean shutdown, wait for waitingTasks to finish", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			r := tempo.NewQueueRunner(tempo.RunnerCfg{Parallelism: 2, QueueSize: 10})
			r.StartBg()

			var result []string
			lock := sync.Mutex{}
			for i := 1; i <= 2; i++ {
				_, err := r.Add(func(ctx context.Context) error {
					select {
					case <-time.After(10 * time.Minute):
						lock.Lock()
						result = append(result, strconv.Itoa(i))
						lock.Unlock()
						// finished normally
						return nil
					case <-ctx.Done():
						// still take some time for shutdown
						time.Sleep(1 * time.Minute)
						lock.Lock()
						result = append(result, strconv.Itoa(i))
						lock.Unlock()
						return nil
					}

				}, "some action")
				if err != nil {
					t.Fatal(err)
				}
			}

			time.Sleep(5 * time.Minute)
			// trigger shutdown before waitingTasks finished,
			// we use a context for the shutdown that does not expire
			err := r.ShutDown(context.Background())
			if err != nil {
				t.Errorf("unable to shut down server: %v", err)
			}

			// verify work after shutdown
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
			r := tempo.NewQueueRunner(tempo.RunnerCfg{Parallelism: 2, QueueSize: 10})
			r.StartBg()

			var result []string
			lock := sync.Mutex{}
			// create 2 waitingTasks that will not finish during the shutdown
			for i := 1; i <= 2; i++ {
				_, err := r.Add(func(ctx context.Context) error {
					select {
					case <-time.After(10 * time.Minute):
						lock.Lock()
						result = append(result, strconv.Itoa(i))
						lock.Unlock()
						// finished normally
						return nil
					case <-ctx.Done():
						// still take some time for shutdown
						time.Sleep(5 * time.Minute)
						lock.Lock()
						result = append(result, strconv.Itoa(i))
						lock.Unlock()
						return nil
					}
				}, "some action")
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
			r := tempo.NewQueueRunner(tempo.RunnerCfg{Parallelism: 2, QueueSize: 20})
			r.StartBg()

			// add 3 jobs
			for i := 1; i <= 4; i++ {
				_, err := r.Add(func(ctx context.Context) error {
					time.Sleep(10 * time.Minute)
					return nil
				}, "some action")
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

func TestRunnerCatchPanic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := tempo.NewQueueRunner(tempo.RunnerCfg{Parallelism: 2, QueueSize: 20})
		r.StartBg()

		// add 3 jobs
		for i := 1; i <= 3; i++ {
			_, err := r.Add(func(ctx context.Context) error {
				time.Sleep(1 * time.Minute)
				panic("panic")
			}, "some action")
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
			r := tempo.NewQueueRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 20})
			r.StartBg()

			fn := func(ctx context.Context) error {
				timer := time.NewTimer(time.Hour * 10)
				defer timer.Stop()

				select {
				case <-ctx.Done():
					// return early
					return nil
				case <-timer.C:
					return nil
				}
			}

			_, err := r.Add(fn, "running task")
			if err != nil {
				t.Fatal(err)
			}

			id, err := r.Add(fn, "waiting task")
			if err != nil {
				t.Fatal(err)
			}

			time.Sleep(3 * time.Minute)

			err = r.Cancel(t.Context(), id)
			if err != nil {
				t.Fatalf("unable to cancel job: %v", err)
			}

			task, err := r.GetTask(id)
			if err != nil {
				t.Fatalf("unable to get task: %v", err)
			}
			if task.Status != tempo.TaskStatusCanceled {
				t.Errorf("expecting task to be in status %s but got %s", tempo.TaskStatusCanceled.Str(), task.Status.Str())
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
			r := tempo.NewQueueRunner(tempo.RunnerCfg{Parallelism: 2, QueueSize: 20})
			r.StartBg()

			msgs := []string{}
			lock := sync.Mutex{}

			// a job that does something every minute, and does a shutdown when the context id canceled
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

						return nil
					case <-ticker.C:
						lock.Lock()
						msgs = append(msgs, fmt.Sprintf("msg %d", i))
						lock.Unlock()
						i++
					}
				}
			}

			id, err := r.Add(fn, "timeout_fn")
			if err != nil {
				t.Fatal(err)
			}

			time.Sleep(3 * time.Minute)

			err = r.Cancel(t.Context(), id)
			if err != nil {
				t.Fatalf("unable to cancel job: %v", err)
			}

			task, err := r.GetTask(id)
			if err != nil {
				t.Fatalf("unable to get task: %v", err)
			}
			if task.Status != tempo.TaskStatusCanceled {
				t.Errorf("expecting task to be in status %s but got %s", tempo.TaskStatusCanceled.Str(), task.Status.Str())
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
			r := tempo.NewQueueRunner(tempo.RunnerCfg{Parallelism: 2, QueueSize: 20})
			r.StartBg()

			msgs := []string{}
			lock := sync.Mutex{}

			taskDone := make(chan any, 1) // done chan to avoid test errors

			// a job that does something every minute, and does a shutdown when the context id canceled
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

						// we ignore the context termination
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

			id, err := r.Add(fn, "timeout_fn")
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

			task, err := r.GetTask(id)
			if err != nil {
				t.Fatalf("unable to get task: %v", err)
			}
			if task.Status != tempo.TaskStatusCancelError {
				t.Errorf("expecting task to be in status %s but got %s", tempo.TaskStatusCancelError.Str(), task.Status.Str())
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
