package tempo_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-bumbu/tempo"
	"github.com/google/uuid"
)

// ExampleQueueRunner is a basic example on how to use the queue runner
func ExampleQueueRunner() {
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("Example 'ExampleQueueRunner' failed due to error :%v", err)
		}
	}()

	queue := tempo.NewTaskQueue(tempo.TaskQueueCfg{QueueSize: 10, HistorySize: 10})
	reg := tempo.NewTaskRegistry()
	for i := range 5 {
		name := fmt.Sprintf("task_%d", i)
		n := name
		reg.Add(name, tempo.TaskDef{
			Run: func(ctx context.Context) error {
				fmt.Printf("Executing task: %s\n", n)
				return nil
			},
		})
	}
	qrun := tempo.NewQueueRunner(tempo.RunnerCfg{
		Parallelism: 2,
		QueueSize:   10,
		HistorySize: 10,
	}, queue, reg)

	qrun.StartBg()

	for i := range 5 {
		name := fmt.Sprintf("task_%d", i)
		_, err := qrun.Add(name)
		if err != nil {
			panic(err)
		}
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

	port1, err := GetFreePort()
	if err != nil {
		panic(err)
	}
	port2, err := GetFreePort()
	if err != nil {
		panic(err)
	}
	queue := tempo.NewTaskQueue(tempo.TaskQueueCfg{QueueSize: 2})
	reg := tempo.NewTaskRegistry()
	reg.Add("server1", tempo.TaskDef{
		Run: func(ctx context.Context) error {
			return httpServer(ctx, port1)
		},
	})
	reg.Add("server2", tempo.TaskDef{
		Run: func(ctx context.Context) error {
			return httpServer(ctx, port2)
		},
	})
	q := tempo.NewQueueRunner(tempo.RunnerCfg{
		Parallelism: 2,
		QueueSize:   2,
	}, queue, reg)

	q.StartBg()

	_, err = q.Add("server1")
	if err != nil {
		panic(err)
	}
	_, err = q.Add("server2")
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

	// output (List returns tasks by queue time, newest first):
	//task "server2" in status running
	//task "server1" in status running
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

// ExampleQueueRunner_filePersistenceAndRestart uses a file-backed persistence,
// simulates a server restart (shutdown then new queue/runner loading from the same store),
// and exercises concurrency (multiple goroutines adding tasks, parallel workers).
func ExampleQueueRunner_filePersistenceAndRestart() {
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("Example failed: %v", err)
		}
	}()

	dir, err := os.MkdirTemp("", "tempo-example-*")
	if err != nil {
		panic(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	storePath := filepath.Join(dir, "tasks")
	persist, err := newFilePersistence(storePath)
	if err != nil {
		panic(err)
	}

	cfg := tempo.TaskQueueCfg{QueueSize: 50, HistorySize: 20}
	queue := tempo.NewTaskQueueWithPersistence(cfg, persist)
	reg := tempo.NewTaskRegistry()
	reg.Add("work", tempo.TaskDef{
		Run: func(ctx context.Context) error {
			time.Sleep(2 * time.Millisecond)
			return nil
		},
	})

	runner := tempo.NewQueueRunner(tempo.RunnerCfg{
		Parallelism: 3,
		QueueSize:   50,
		HistorySize: 20,
	}, queue, reg)
	runner.StartBg()

	// Concurrent adds from multiple goroutines
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for j := 0; j < 6; j++ {
				_, _ = runner.Add("work")
				time.Sleep(1 * time.Millisecond)
			}
		}(i)
	}
	wg.Wait()

	time.Sleep(30 * time.Millisecond)
	list1 := runner.List()
	fmt.Printf("before shutdown: %d tasks\n", len(list1))

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	err = runner.ShutDown(shutdownCtx)
	cancel()
	if err != nil {
		panic(err)
	}

	// Simulate server restart: new queue loading from same file store, new runner
	persist2, err := newFilePersistence(storePath)
	if err != nil {
		panic(err)
	}
	queue2 := tempo.NewTaskQueueWithPersistence(cfg, persist2)
	runner2 := tempo.NewQueueRunner(tempo.RunnerCfg{
		Parallelism: 3,
		QueueSize:   50,
		HistorySize: 20,
	}, queue2, reg)
	runner2.StartBg()

	list2 := runner2.List()
	fmt.Printf("after restart (recovered): %d tasks\n", len(list2))

	// Add more tasks concurrently after restart
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 4; j++ {
				_, _ = runner2.Add("work")
			}
		}()
	}
	wg.Wait()
	time.Sleep(25 * time.Millisecond)

	list3 := runner2.List()
	fmt.Printf("after concurrent adds: %d tasks\n", len(list3))

	shutdownCtx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	_ = runner2.ShutDown(shutdownCtx2)
	cancel2()

	// Output lines (task counts depend on timing):
	// before shutdown: N tasks
	// after restart (recovered): N tasks
	// after concurrent adds: M tasks
}

// filePersistence stores task state in a directory as one JSON file per task.
// It implements tempo.TaskStatePersistence and tempo.RecoverablePersistence for the example.
type filePersistence struct {
	dir string
	mu  sync.Mutex
}

func newFilePersistence(dir string) (*filePersistence, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	return &filePersistence{dir: dir}, nil
}

func (f *filePersistence) path(id uuid.UUID) string {
	return filepath.Join(f.dir, id.String()+".json")
}

func (f *filePersistence) SaveTask(ctx context.Context, task tempo.TaskInfo) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	p := f.path(task.ID)
	data, err := json.Marshal(task)
	if err != nil {
		return err
	}
	return os.WriteFile(p, data, 0o600)
}

func (f *filePersistence) RemoveTasks(ctx context.Context, ids []uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range ids {
		_ = os.Remove(f.path(id))
	}
	return nil
}

func (f *filePersistence) List(ctx context.Context) ([]tempo.TaskInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	entries, err := os.ReadDir(f.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []tempo.TaskInfo
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(f.dir, e.Name()))
		if err != nil {
			continue
		}
		var task tempo.TaskInfo
		if err := json.Unmarshal(data, &task); err != nil {
			continue
		}
		out = append(out, task)
	}
	return out, nil
}
