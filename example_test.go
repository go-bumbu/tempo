package tempo_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-bumbu/tempo"
	"net"
	"net/http"
	"time"
)

// ExampleQueueRunner is a basic example on how to use the queue runner
func ExampleQueueRunner() {
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("Example 'ExampleQueueRunner' failed due to error :%v", err)
		}
	}()

	qrun := tempo.NewQueueRunner(tempo.RunnerCfg{
		Parallelism: 2,
		QueueSize:   10,
		HistorySize: 10,
	})

	// start in the background
	qrun.StartBg()

	// the task function
	fn := func(name string) func(context.Context) error {
		return func(ctx context.Context) error {
			fmt.Printf("Executing task: %s\n", name)
			return nil
		}
	}
	// add the task to be processed
	for i := range 5 {
		name := fmt.Sprintf("task_%d", i)
		_, err := qrun.Add(fn(name), name)
		if err != nil {
			panic(err)
		}
		// add small delay to ensure execution order
		time.Sleep(5 * time.Millisecond)
	}

	// clean shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := qrun.ShutDown(shutdownCtx)
	if err != nil {
		panic(err)
	}

	// output:
	//Executing task: task_0
	//Executing task: task_1
	//Executing task: task_2
	//Executing task: task_3
	//Executing task: task_4
}

// ExampleQueueRunner_runHttpServer is a more complex scenario that showcases how to use the queue runner
// to start 2 http servers, and do a clean shutdown of both
func ExampleQueueRunner_runHttpServer() {

	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("Example 'ExampleQueueRunner' failed due to error :%v", err)
		}
	}()

	q := tempo.NewQueueRunner(tempo.RunnerCfg{
		Parallelism: 2,
		QueueSize:   2,
	})

	// start in the background
	q.StartBg()

	port1, err := GetFreePort()
	if err != nil {
		panic(err)
	}

	// add the first server
	_, err = q.Add(func(ctx context.Context) error {
		err := httpServer(ctx, port1)
		if err != nil {
			panic(err)
		}
		return nil
	}, "server1")
	if err != nil {
		panic(err)
	}

	// add the second server
	port2, err := GetFreePort()
	if err != nil {
		panic(err)
	}
	_, err = q.Add(func(ctx context.Context) error {
		err := httpServer(ctx, port2)
		if err != nil {
			panic(err)
		}
		return nil
	}, "server2")
	if err != nil {
		panic(err)
	}

	// give the servers some time to start
	time.Sleep(300 * time.Millisecond)

	// test that they are reachable by http
	err = testHttpRequest(port1)
	if err != nil {
		panic(fmt.Sprintf("unable to connect to server 1: %v", err))
	}
	err = testHttpRequest(port2)
	if err != nil {
		panic(fmt.Sprintf("unable to connect to server 2: %v", err))
	}

	// list all tasks
	tasks := q.List()
	for _, task := range tasks {
		fmt.Printf("task \"%s\" in status %s\n", task.Name, task.Status.Str())
	}

	err = q.ShutDown(context.Background())
	if err != nil {
		panic(fmt.Sprintf("error shutting down server: %v", err))
	}

	// test that they are NOT reachable by http after shutdown
	err = testHttpRequest(port1)
	if err == nil {
		panic("server should be stopped after shutdown")
	}
	err = testHttpRequest(port2)
	if err == nil {
		panic("server should be stopped after shutdown")
	}

	// output:
	//task "server1" in status running
	//task "server2" in status running
}

// httpServer is a small helper function that stars a dummy http server on the given port
// if the context is terminated, it will shut down the server
func httpServer(ctx context.Context, port int) error {

	srv := http.Server{
		Addr: fmt.Sprintf(":%d", port),
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprintf(w, "Hello from port %d\n", port)
		}),
		ReadHeaderTimeout: 5 * time.Second,
	}

	// StartBg server in goroutine
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- srv.ListenAndServe()
	}()

	// Wait for server to be ready or context cancellation
	select {
	case <-time.After(200 * time.Millisecond):
		// No error after short delay → server started
	case err := <-serverErr:
		panic(fmt.Errorf("server failed to start: %w", err))
	case <-ctx.Done():
		fmt.Printf("server canceled during starupt: %v", ctx.Err())
		panic(ctx.Err())
	}

	// Wait for cancellation
	<-ctx.Done()

	// Graceful shutdown with timeout
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		panic(fmt.Errorf("shutdown failed: %w", err))
	}

	// Check if ListenAndServe returned an error
	select {
	case err := <-serverErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			panic(fmt.Errorf("server error: %w", err))
		}
	default:
	}
	return nil
}

// GetFreePort asks the kernel for a free open port that is ready to use.
func GetFreePort() (port int, err error) {
	var a *net.TCPAddr
	if a, err = net.ResolveTCPAddr("tcp", "localhost:0"); err == nil {
		var l *net.TCPListener
		if l, err = net.ListenTCP("tcp", a); err == nil {
			defer func() {
				_ = l.Close()
			}()
			return l.Addr().(*net.TCPAddr).Port, nil
		}
	}
	return
}

func testHttpRequest(port int) error {
	client := &http.Client{
		Timeout: 2 * time.Second,
	}

	resp, err := client.Get(fmt.Sprintf("http://localhost:%d/", port))
	if err != nil {
		return fmt.Errorf("cannot get response: %v", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}
