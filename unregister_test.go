package tempo_test

import (
	"context"
	"errors"
	"log/slog"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-bumbu/tempo"
	"github.com/google/uuid"
)

// blockUntilCanceled runs until its ctx is canceled and returns ctx.Err(), so a
// canceled run records canceled.
func blockUntilCanceled(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error {
	<-ctx.Done()
	return ctx.Err()
}

// TestRunnerUnregisterCancelsWaitingTasks guards that the waiting tasks of an
// unregistered name end canceled, instead of waiting for a worker to claim them
// and fail them for want of a handler, and that other names' tasks stay waiting.
func TestRunnerUnregisterCancelsWaitingTasks(t *testing.T) {
	// Not started, so every task stays waiting until canceled.
	r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5})
	r.RegisterRaw("job-1", blockUntilCanceled)
	r.RegisterRaw("job-2", blockUntilCanceled)
	var ids []uuid.UUID
	for _, name := range []string{"job-1", "job-1", "job-2"} {
		id, _, err := r.AddRaw(name, nil)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}

	if err := r.Unregister(context.Background(), "job-1"); err != nil {
		t.Fatalf("Unregister: %v", err)
	}
	for _, id := range ids[:2] {
		info, _ := r.GetTask(id)
		if info.Status != tempo.TaskStatusCanceled {
			t.Errorf("waiting task of the unregistered name: got %s, want canceled", info.Status.Str())
		}
		if info.EndedAt.IsZero() {
			t.Error("waiting task of the unregistered name: no end time")
		}
	}
	if info, _ := r.GetTask(ids[2]); info.Status != tempo.TaskStatusWaiting {
		t.Errorf("waiting task of another name: got %s, want waiting", info.Status.Str())
	}
}

// TestRunnerUnregisterStoresWaitingTasksCanceled guards that canceling the
// waiting tasks reaches the store even when Unregister's ctx is already done, as
// when the request that deleted the task's entity went away. On that ctx, a store
// honouring it would fail the write and keep the tasks waiting, and they would
// run after a restart.
func TestRunnerUnregisterStoresWaitingTasksCanceled(t *testing.T) {
	store := ctxAwareStore{newRecoverableMem()}
	// Neither runner is started, so the task stays waiting until canceled.
	r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5, Persistence: store})
	r.RegisterRaw("job", blockUntilCanceled)
	id, _, err := r.AddRaw("job", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Unregister(ctx, "job"); err != nil {
		t.Fatalf("Unregister: %v", err)
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

// TestRunnerUnregisterWithoutAHandler guards that unregistering a name with no
// handler is not an error and still cancels the name's waiting tasks, such as
// ones a restart loaded before the name was registered again.
func TestRunnerUnregisterWithoutAHandler(t *testing.T) {
	// Not started, so the task stays waiting until canceled.
	r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5})
	id, _, err := r.AddRaw("job", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := r.Unregister(context.Background(), "job"); err != nil {
		t.Errorf("Unregister of a name with no handler: %v", err)
	}
	if info, _ := r.GetTask(id); info.Status != tempo.TaskStatusCanceled {
		t.Errorf("waiting task of a name with no handler: got %s, want canceled", info.Status.Str())
	}
}

// TestRunnerUnregisterRemovesTheHandler guards that Unregister removes the
// name's handler and no other: a task enqueued under the name afterwards does
// not run the handler registered for it before, and fails, as for any name
// without a handler, while a task of another name still runs.
func TestRunnerUnregisterRemovesTheHandler(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5})
		var ran atomic.Bool
		r.RegisterRaw("job", func(context.Context, *slog.Logger, tempo.Progress, []byte) error {
			ran.Store(true)
			return nil
		})
		r.RegisterRaw("other", func(context.Context, *slog.Logger, tempo.Progress, []byte) error {
			return nil
		})
		r.StartBg()

		if err := r.Unregister(context.Background(), "job"); err != nil {
			t.Fatalf("Unregister: %v", err)
		}
		job, _, err := r.AddRaw("job", nil)
		if err != nil {
			t.Fatal(err)
		}
		other, _, err := r.AddRaw("other", nil)
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait()

		if ran.Load() {
			t.Error("the unregistered handler ran")
		}
		if info, _ := r.GetTask(job); info.Status != tempo.TaskStatusFailed {
			t.Errorf("task enqueued after Unregister: got %s, want failed", info.Status.Str())
		}
		if info, _ := r.GetTask(other); info.Status != tempo.TaskStatusComplete {
			t.Errorf("task of another name: got %s, want complete", info.Status.Str())
		}
		if err := r.ShutDown(context.Background()); err != nil {
			t.Fatalf("ShutDown: %v", err)
		}
	})
}

// TestRunnerUnregisterLeavesFinishedTasks guards that Unregister cancels only
// the name's waiting and running tasks: a finished one keeps the status and end
// time it ended with.
func TestRunnerUnregisterLeavesFinishedTasks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5})
		r.RegisterRaw("job", func(context.Context, *slog.Logger, tempo.Progress, []byte) error {
			return nil
		})
		r.StartBg()
		id, _, err := r.AddRaw("job", nil)
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		before, _ := r.GetTask(id)
		if before.Status != tempo.TaskStatusComplete {
			t.Fatalf("precondition: got %s, want complete", before.Status.Str())
		}

		time.Sleep(time.Minute) // so a rewritten end time would differ
		if err := r.Unregister(context.Background(), "job"); err != nil {
			t.Fatalf("Unregister: %v", err)
		}
		if after, _ := r.GetTask(id); after.Status != tempo.TaskStatusComplete || !after.EndedAt.Equal(before.EndedAt) {
			t.Errorf("finished task: got %s ended %v, want complete ended %v", after.Status.Str(), after.EndedAt, before.EndedAt)
		}
		if err := r.ShutDown(context.Background()); err != nil {
			t.Fatalf("ShutDown: %v", err)
		}
	})
}

// TestRunnerUnregisterCancelsRunningTasks guards that Unregister cancels the
// name's running tasks and returns only once they have stopped, with the status
// their return value gives in place, while other names' runs keep going. The
// canceled runs are held stopping until release, so a return before then is
// caught.
func TestRunnerUnregisterCancelsRunningTasks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		stopsOnRelease := func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error {
			<-ctx.Done()
			<-release
			return ctx.Err()
		}
		r := newTestRunner(tempo.RunnerCfg{Parallelism: 3, QueueSize: 5})
		r.RegisterRaw("job-1", stopsOnRelease)
		r.RegisterRaw("job-2", stopsOnRelease)
		r.StartBg()
		var ids []uuid.UUID
		for _, name := range []string{"job-1", "job-1", "job-2"} {
			id, _, err := r.AddRaw(name, nil)
			if err != nil {
				t.Fatal(err)
			}
			ids = append(ids, id)
		}
		synctest.Wait() // all three are running, parked on ctx.Done

		var unregisterErr error
		returned := make(chan struct{})
		go func() {
			unregisterErr = r.Unregister(context.Background(), "job-1")
			close(returned)
		}()
		synctest.Wait() // job-1's runs are canceled and held stopping
		select {
		case <-returned:
			t.Error("Unregister returned before the canceled runs stopped")
		default:
		}

		close(release)
		<-returned
		if unregisterErr != nil {
			t.Fatalf("Unregister: %v", unregisterErr)
		}
		for _, id := range ids[:2] {
			if info, _ := r.GetTask(id); info.Status != tempo.TaskStatusCanceled {
				t.Errorf("running task of the unregistered name when Unregister returned: got %s, want canceled", info.Status.Str())
			}
		}
		if info, _ := r.GetTask(ids[2]); info.Status != tempo.TaskStatusRunning {
			t.Errorf("running task of another name: got %s, want running", info.Status.Str())
		}
		if err := r.ShutDown(context.Background()); err != nil {
			t.Fatalf("ShutDown: %v", err)
		}
	})
}

// TestRunnerUnregisterCancelsWaitingTasksBeforeStoppingRuns guards that the
// name's waiting tasks are canceled before its runs are stopped. Stopping a run
// frees its worker, which would otherwise claim the next waiting task of the
// name and run it past Unregister. The run is held stopping until release, so
// the waiting task can be read while Unregister waits on the run.
func TestRunnerUnregisterCancelsWaitingTasksBeforeStoppingRuns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var started atomic.Int64
		r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5})
		r.RegisterRaw("job", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error {
			started.Add(1)
			<-ctx.Done()
			<-release
			return ctx.Err()
		})
		r.StartBg()
		running, _, err := r.AddRaw("job", nil)
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait() // the only worker runs it, parked on ctx.Done
		waiting, _, err := r.AddRaw("job", nil)
		if err != nil {
			t.Fatal(err)
		}

		var unregisterErr error
		returned := make(chan struct{})
		go func() {
			unregisterErr = r.Unregister(context.Background(), "job")
			close(returned)
		}()
		synctest.Wait() // the run is canceled and held stopping
		if info, _ := r.GetTask(waiting); info.Status != tempo.TaskStatusCanceled {
			t.Errorf("waiting task while Unregister waits on the run: got %s, want canceled", info.Status.Str())
		}

		close(release)
		<-returned
		if unregisterErr != nil {
			t.Fatalf("Unregister: %v", unregisterErr)
		}
		if info, _ := r.GetTask(running); info.Status != tempo.TaskStatusCanceled {
			t.Errorf("running task: got %s, want canceled", info.Status.Str())
		}
		if info, _ := r.GetTask(waiting); info.Status != tempo.TaskStatusCanceled || !info.StartedAt.IsZero() {
			t.Errorf("waiting task once Unregister returned: got %s, started %v; want canceled, never started", info.Status.Str(), info.StartedAt)
		}
		if n := started.Load(); n != 1 {
			t.Errorf("handler runs: got %d, want 1", n)
		}
		if err := r.ShutDown(context.Background()); err != nil {
			t.Fatalf("ShutDown: %v", err)
		}
	})
}

// TestRunnerUnregisterStopsACancelErrorRun guards that Unregister counts a task
// in cancel_error, still running after a Cancel gave up on it, among the runs
// it stops, and returns only once that run has stopped too.
func TestRunnerUnregisterStopsACancelErrorRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5})
		r.RegisterRaw("job", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error {
			<-ctx.Done()
			<-release
			return ctx.Err()
		})
		r.StartBg()
		id, _, err := r.AddRaw("job", nil)
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait() // the task is running, parked on ctx.Done

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := r.Cancel(ctx, id); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Cancel: got %v, want a timeout", err)
		}
		if info, _ := r.GetTask(id); info.Status != tempo.TaskStatusCancelError {
			t.Fatalf("precondition: got %s, want cancel_error", info.Status.Str())
		}

		var unregisterErr error
		returned := make(chan struct{})
		go func() {
			unregisterErr = r.Unregister(context.Background(), "job")
			close(returned)
		}()
		synctest.Wait()
		select {
		case <-returned:
			t.Error("Unregister returned before the cancel_error run stopped")
		default:
		}

		close(release)
		<-returned
		if unregisterErr != nil {
			t.Fatalf("Unregister: %v", unregisterErr)
		}
		if info, _ := r.GetTask(id); info.Status != tempo.TaskStatusCanceled {
			t.Errorf("status when Unregister returned: got %s, want canceled", info.Status.Str())
		}
		if err := r.ShutDown(context.Background()); err != nil {
			t.Fatalf("ShutDown: %v", err)
		}
	})
}

// TestRunnerUnregisterRacingAClaim guards Unregister against a worker claiming
// one of the name's tasks at the same moment. A task claimed before Unregister
// runs its handler until Unregister cancels it; one claimed after it is not
// there to claim. A claim that saw the handler gone fails the task instead,
// which happened when the handler was removed before the waiting tasks were
// canceled, or looked up again after the claim. Both windows are narrow, so the
// test unregisters many names the moment their task is enqueued while several
// workers claim them.
func TestRunnerUnregisterRacingAClaim(t *testing.T) {
	const workers, unregisterers, perUnregisterer = 8, 8, 200
	// An unregisterer adds again only once its Unregister returned, so at most
	// one task per unregisterer is ever waiting: a QueueSize of unregisterers
	// never fills.
	r := newTestRunner(tempo.RunnerCfg{Parallelism: workers, QueueSize: unregisterers})
	var live atomic.Int64 // runs started and not yet returned
	handler := func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error {
		live.Add(1)
		defer live.Add(-1)
		<-ctx.Done()
		return ctx.Err()
	}
	r.StartBg()

	var failed atomic.Int64
	var wg sync.WaitGroup
	for i := range unregisterers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			name := "job-" + strconv.Itoa(i)
			for range perUnregisterer {
				r.RegisterRaw(name, handler)
				if _, _, err := r.AddRaw(name, nil); err != nil {
					t.Error(err)
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err := r.Unregister(ctx, name)
				cancel()
				if err != nil && failed.Add(1) <= 3 {
					t.Errorf("Unregister right after an enqueue: %v", err)
				}
			}
		}()
	}
	wg.Wait()

	if n := failed.Load(); n > 0 {
		t.Errorf("%d of %d Unregister calls failed", n, unregisterers*perUnregisterer)
	}
	// Unregister returns only once the runs it canceled have stopped, and a task
	// it canceled waiting never starts: with every Unregister returned, no run
	// may be left.
	if n := live.Load(); n != 0 {
		t.Errorf("%d runs still going after every Unregister returned", n)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.ShutDown(ctx); err != nil {
		t.Fatalf("ShutDown: %v", err)
	}
	// Each task was unregistered waiting or running, so each ends canceled: none
	// failed for want of a handler.
	statuses := map[string]int{}
	for _, info := range r.List() {
		if info.Status != tempo.TaskStatusCanceled {
			statuses[info.Status.Str()]++
		}
	}
	if len(statuses) > 0 {
		t.Errorf("tasks not canceled once the runner stopped, by status: %v", statuses)
	}
}

// TestRunnerUnregisterRacingAnEnqueue guards the same single step from the side
// of an enqueue: a task enqueued while Unregister runs is canceled with the
// rest, or finds no handler and fails; it never runs the handler past
// Unregister. Removing the handler after that step, outside the queue lock, let
// a task enqueued in between run the old handler with nothing left to stop it.
// The window is narrow, so each round enqueues the name in a loop while it is
// unregistered.
func TestRunnerUnregisterRacingAnEnqueue(t *testing.T) {
	const workers, names, rounds = 32, 8, 200
	r := newTestRunner(tempo.RunnerCfg{Parallelism: workers, QueueSize: 64})
	r.StartBg()

	var escaped atomic.Int64 // runs that went past the Unregister of their round
	var wg sync.WaitGroup
	for i := range names {
		wg.Add(1)
		go func() {
			defer wg.Done()
			name := "job-" + strconv.Itoa(i)
			for range rounds {
				// Each round registers a handler of its own, so its runs are
				// told apart from those of the next round.
				var live atomic.Int64
				var unregistered atomic.Bool
				r.RegisterRaw(name, func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error {
					if unregistered.Load() {
						escaped.Add(1) // started after Unregister returned
					}
					live.Add(1)
					defer live.Add(-1)
					<-ctx.Done()
					return ctx.Err()
				})
				stop := make(chan struct{})
				stopped := make(chan struct{})
				go func() {
					defer close(stopped)
					for {
						select {
						case <-stop:
							return
						default:
							_, _, _ = r.AddRaw(name, nil) // a full queue is fine
							runtime.Gosched()
						}
					}
				}()

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err := r.Unregister(ctx, name)
				cancel()
				unregistered.Store(true)
				if err != nil {
					t.Errorf("Unregister while the name is enqueued: %v", err)
				}
				if live.Load() != 0 {
					escaped.Add(1) // still running after Unregister returned
				}
				close(stop)
				<-stopped
			}
		}()
	}
	wg.Wait()

	if n := escaped.Load(); n > 0 {
		t.Errorf("%d runs of an unregistered handler went past Unregister", n)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.ShutDown(ctx); err != nil {
		t.Fatalf("ShutDown: %v", err)
	}
}

// TestRunnerUnregisterTimeout guards that Unregister gives up once its ctx is
// done before a canceled run stops, as Cancel does: it returns a timeout, and the
// run shows cancel_error until it stops, then the status its return value gives.
func TestRunnerUnregisterTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		r := newTestRunner(tempo.RunnerCfg{Parallelism: 1, QueueSize: 5})
		r.RegisterRaw("job", func(ctx context.Context, _ *slog.Logger, _ tempo.Progress, _ []byte) error {
			<-ctx.Done()
			<-release
			return ctx.Err()
		})
		r.StartBg()
		id, _, err := r.AddRaw("job", nil)
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait() // the task is running, parked on ctx.Done

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err = r.Unregister(ctx, "job")
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Unregister on a run that outlasts its ctx: got %v, want a timeout", err)
		}
		if info, _ := r.GetTask(id); info.Status != tempo.TaskStatusCancelError {
			t.Errorf("status while the run is stopping: got %s, want cancel_error", info.Status.Str())
		}

		close(release)
		if err := r.ShutDown(context.Background()); err != nil {
			t.Fatalf("ShutDown: %v", err)
		}
		if info, _ := r.GetTask(id); info.Status != tempo.TaskStatusCanceled {
			t.Errorf("status once the run stopped: got %s, want canceled", info.Status.Str())
		}
	})
}
