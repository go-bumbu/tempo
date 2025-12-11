package tempo_test

import (
	"context"
	"errors"
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
			r := tempo.NewRunner(tempo.TaskQueueCfg{MaxParallelism: 1, QueueSize: 10})
			r.Run()

			var result []string
			lock := sync.Mutex{}
			for i := 1; i <= 4; i++ {
				n := i
				_, err := r.Add(func(ctx context.Context) {
					lock.Lock()
					result = append(result, strconv.Itoa(n))
					lock.Unlock()
					time.Sleep(10 * time.Minute)
				})
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
			r := tempo.NewRunner(tempo.TaskQueueCfg{MaxParallelism: 3, QueueSize: 20})
			r.Run()

			var result []string
			lock := sync.Mutex{}
			for i := 1; i <= 12; i++ {
				_, err := r.Add(func(ctx context.Context) {
					lock.Lock()
					result = append(result, strconv.Itoa(i))
					lock.Unlock()
					time.Sleep(10 * time.Minute)
				})
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
		r := tempo.NewRunner(tempo.TaskQueueCfg{MaxParallelism: 3, QueueSize: 5})
		r.Run()

		// fill queue up to capacity
		// TODO ensure the test actually covers the expectation
		for i := 1; i <= 5; i++ {
			_, err := r.Add(func(ctx context.Context) {
				time.Sleep(10 * time.Minute)
			})
			if err != nil {
				t.Fatal(err)
			}
		}

		_, err := r.Add(func(ctx context.Context) {
			time.Sleep(10 * time.Minute)
		})

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
			r := tempo.NewRunner(tempo.TaskQueueCfg{MaxParallelism: 2, QueueSize: 10})
			r.Run()

			var result []string
			lock := sync.Mutex{}
			for i := 1; i <= 2; i++ {
				_, err := r.Add(func(ctx context.Context) {
					select {
					case <-time.After(10 * time.Minute):
						lock.Lock()
						result = append(result, strconv.Itoa(i))
						lock.Unlock()
						// finished normally
						return
					case <-ctx.Done():
						// still take some time for shutdown
						time.Sleep(1 * time.Minute)
						lock.Lock()
						result = append(result, strconv.Itoa(i))
						lock.Unlock()
						return
					}
				})
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
			r := tempo.NewRunner(tempo.TaskQueueCfg{MaxParallelism: 2, QueueSize: 10})
			r.Run()

			var result []string
			lock := sync.Mutex{}
			// create 2 waitingTasks that will not finish during the shutdown
			for i := 1; i <= 2; i++ {
				_, err := r.Add(func(ctx context.Context) {
					select {
					case <-time.After(10 * time.Minute):
						lock.Lock()
						result = append(result, strconv.Itoa(i))
						lock.Unlock()
						// finished normally
						return
					case <-ctx.Done():
						// still take some time for shutdown
						time.Sleep(5 * time.Minute)
						lock.Lock()
						result = append(result, strconv.Itoa(i))
						lock.Unlock()
						return
					}
				})
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

func TestRunnerListJobs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := tempo.NewRunner(tempo.TaskQueueCfg{MaxParallelism: 2, QueueSize: 20})
		r.Run()

		// add 3 jobs
		for i := 1; i <= 4; i++ {
			_, err := r.Add(func(ctx context.Context) {
				time.Sleep(10 * time.Minute)
			})
			if err != nil {
				t.Fatal(err)
			}
		}

		// wait to get the tasks scheduled
		time.Sleep(1 * time.Minute)

		gotStatus := getRunnerJobStatus(r.List())
		want := []string{"running", "running", "waiting"}
		if diff := cmp.Diff(gotStatus, want); diff != "" {
			t.Errorf("unexpected value (-got +want)\n%s", diff)
		}

		//// wait 11 minutes to have the firs 2 jobs finish
		//time.Sleep(11 * time.Minute)
		//
		//gotStatus = getRunnerJobStatus(r.List())
		//want = []string{"completed", "completed", "running"}
		//if diff := cmp.Diff(gotStatus, want); diff != "" {
		//	t.Errorf("unexpected value (-got +want)\n%s", diff)
		//}

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

func TestRunnerCatchPanic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := tempo.NewRunner(tempo.TaskQueueCfg{MaxParallelism: 2, QueueSize: 20})
		r.Run()

		// add 3 jobs
		for i := 1; i <= 3; i++ {
			_, err := r.Add(func(ctx context.Context) {
				time.Sleep(1 * time.Minute)
				panic("panic")
			})
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

func getRunnerJobStatus(in []tempo.QueueTaskInfo) []string {
	r := []string{}
	for _, item := range in {
		r = append(r, string(item.Status))
	}
	return r
}

// TODO, test for:

// cancel single job
// test throw error if task is added before the shceduler was stareted
// get job staus after completion => ok, error, panic
// set name of the task
