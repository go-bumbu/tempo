package tempo_test

import (
	"context"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/go-bumbu/tempo"
	"go.uber.org/goleak"
	"net/http"
	"os"
	"testing"
	"testing/synctest"
	"time"
)

// Helper: a task that increments a counter
//func makeCounterTask(counter *int, mu *sync.Mutex, done chan struct{}) Task {
//	return func(ctx context.Context) {
//		mu.Lock()
//		*counter++
//		mu.Unlock()
//		if done != nil {
//			done <- struct{}{}
//		}
//	}
//}

//func TestWorkerPool_TaskExecution(t *testing.T) {
//	counter := 0
//	var mu sync.Mutex
//	done := make(chan struct{}, 3)
//
//	pool := NewWorkerPool(3, 5)
//	defer pool.ShutDown()
//
//	// EnQueue 3 tasksChan
//	for i := 0; i < 3; i++ {
//		err := pool.EnQueue(makeCounterTask(&counter, &mu, done))
//		if err != nil {
//			t.Fatalf("unexpected error scheduling task: %v", err)
//		}
//	}
//
//	// Wait for tasksChan to complete
//	for i := 0; i < 3; i++ {
//		<-done
//	}
//
//	if counter != 3 {
//		t.Errorf("expected counter to be 3, got %d", counter)
//	}
//}

//func TestWorkerPool_NonBlockingSchedule(t *testing.T) {
//	pool := NewWorkerPool(2, 2) // 2 poolSize, queue buffer 2
//	defer pool.ShutDown()
//
//	blockTask := func(ctx context.Context) {
//		time.Sleep(50 * time.Millisecond)
//	}
//
//	// Fill the queue (2 poolSize + 2 buffer = 4 slots)
//	for i := 0; i < 4; i++ {
//		err := pool.EnQueue(blockTask)
//		if err != nil {
//			t.Fatalf("unexpected error scheduling task %d: %v", i, err)
//		}
//	}
//
//	// Next schedule should return error (queue full)
//	err := pool.EnQueue(blockTask)
//	if err == nil {
//		t.Errorf("expected error scheduling task when queue full, got nil")
//	}
//}

func TestMain(m *testing.M) {

	// main block that runs tests
	code := m.Run()

	//check for routine leaks
	opts := []goleak.Option{
		goleak.IgnoreAnyFunction(""),
	}
	if err := goleak.Find(opts...); err != nil {
		fmt.Printf("found routine leak: %v\n", err)
		os.Exit(1)
	}
	os.Exit(code)
}

func TestHttpServer(t *testing.T) {
	t.Run("async shutdown", func(t *testing.T) {
		srv := NewHttpServerRunner()

		pool := tempo.NewWorkerPool()
		pool.Start()

		err := pool.EnQueue(srv)
		if err != nil {
			t.Fatal(err)
		}

		// test that the server is active
		err = testHttpRequest(srv.Port)
		if err != nil {
			t.Errorf("unable to contact server: %v", err)
		}

		go func() {
			// wait before running shutdown, this simulates a signal listener like
			// signal.Notify(make(chan os.Signal, 1), syscall.SIGINT, syscall.SIGTERM)
			time.Sleep(200 * time.Millisecond)
			err = pool.ShutDown()
			if err != nil {
				t.Errorf("unable to shut down server: %v", err)
			}
		}()
		pool.Wait()

		err = testHttpRequest(srv.Port)
		if err == nil {
			t.Errorf("server should be stoped after shutdown")
		}
	})

	t.Run("sync shutdown", func(t *testing.T) {
		srv := NewHttpServerRunner()

		pool := tempo.NewWorkerPool()
		pool.Start()

		err := pool.EnQueue(srv)
		if err != nil {
			t.Fatal(err)
		}

		// test that the server is active
		err = testHttpRequest(srv.Port)
		if err != nil {
			t.Errorf("unable to contact server: %v", err)
		}

		// give the server time to start
		// normally other tasks would be blocking here, e.g. a web server
		time.Sleep(100 * time.Millisecond)

		// verify no routines are leaking after the sync shutdown
		err = pool.ShutDown()
		if err != nil {
			t.Errorf("unable to shut down server: %v", err)
		}

		err = testHttpRequest(srv.Port)
		if err == nil {
			t.Errorf("server should be stoped after shutdown")
		}
	})

}

func testHttpRequest(port int) error {
	client := &http.Client{
		Timeout: 2 * time.Second,
	}

	resp, err := client.Get(fmt.Sprintf("http://localhost:%d/", port))
	if err != nil {
		return fmt.Errorf("cannot get response: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}

func TestTaskRunner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool := tempo.NewWorkerPool()
		pool.Start()

		task := SampleTaskRunner{
			Items: []string{"task1", "task2"},
		}

		err := pool.EnQueue(&task)
		if err != nil {
			t.Fatal(err)
		}

		go func() {
			// wait before running shutdown, this simulates a signal listener like
			// signal.Notify(make(chan os.Signal, 1), syscall.SIGINT, syscall.SIGTERM)
			time.Sleep(5 * time.Minute)
			err = pool.ShutDown()
			if err != nil {
				t.Errorf("unable to shut down server: %v", err)
			}
		}()
		pool.Wait()

		spew.Dump(task.ProcessedItems)
	})
}

//func TestSomething(t *testing.T) {
//	//synctest.Test(t, func(t *testing.T) {
//	pool := tempo.NewWorkerPool()
//	defer func() {
//		err := pool.ShutDown()
//		if err != nil {
//			t.Log(err)
//		}
//	}()
//
//	pool.Start()
//
//	task := sampleTask{}
//	err := pool.EnQueue(&task)
//	if err != nil {
//		t.Fatal(err.Error())
//	}
//
//	fmt.Println("waitng")
//	time.Sleep(2 * time.Second)
//	fmt.Println("done waiting")
//	//})
//}

//func TestWorkerPool_StopCancelsWorkers(t *testing.T) {
//	counter := 0
//	var mu sync.Mutex
//	pool := NewWorkerPool(2, 5)
//
//	// EnQueue long-running tasksChan
//	for i := 0; i < 2; i++ {
//		_ = pool.EnQueue(func(ctx context.Context) {
//			select {
//			case <-ctx.Done():
//				return
//			case <-time.After(100 * time.Millisecond):
//				mu.Lock()
//				counter++
//				mu.Unlock()
//			}
//		})
//	}
//
//	// ShutDown pool immediately
//	//here
//	// TODO: don't cancel task immediately, but use a timeout in the context, and make it so that the
//	// tasksChan can be canceled / safely shutdown
//	pool.ShutDown()
//
//	// Wait a short while to ensure poolSize exit
//	time.Sleep(50 * time.Millisecond)
//
//	// Counter should still be 0 because tasksChan were canceled
//	mu.Lock()
//	defer mu.Unlock()
//	if counter != 0 {
//		t.Errorf("expected counter 0 after ShutDown, got %d", counter)
//	}
//}

//func TestWorkerPool_ContextPropagation(t *testing.T) {
//	done := make(chan struct{}, 1)
//	pool := NewWorkerPool(1, 1)
//	defer pool.ShutDown()
//
//	// EnQueue a task that checks context
//	_ = pool.EnQueue(func(ctx context.Context) {
//		if ctx.Err() != nil {
//			t.Errorf("context should not be canceled yet")
//		}
//		done <- struct{}{}
//	})
//
//	select {
//	case <-done:
//	case <-time.After(time.Second):
//		t.Errorf("task did not run")
//	}
//}

type sampleTask struct {
}

func (t *sampleTask) Run(ctx context.Context) error {
	// Break work into chunks and check context regularly
	for {
		select {
		case <-ctx.Done():
			fmt.Println("got context done, shutting down, takes 4 seconds")
			time.Sleep(4 * time.Second)
			return ctx.Err()
		default:
			fmt.Println("performing a task")
			time.Sleep(1 * time.Second)
		}
	}
}
