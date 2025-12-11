package tempo_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-bumbu/tempo"
	"net"
	"net/http"
	"testing"
	"time"
)

// this is a sample on how to use tempo to start and stop multiple http servers
// this is just a demonstration of a more complex use-case
func TestRunMultipleHttpServers(t *testing.T) {
	q := tempo.NewQueueRunner(tempo.QueueCfg{MaxParallelism: 2, QueueSize: 2})
	q.StartBg()

	port1, err := GetFreePort()
	if err != nil {
		panic(err)
	}

	// add the first server
	_, err = q.Add(func(ctx context.Context) {
		err := httpServer(ctx, port1)
		if err != nil {
			panic(err)
		}
	}, "server1")
	if err != nil {
		t.Fatal(err)
	}

	// add the second server
	port2, err := GetFreePort()
	if err != nil {
		panic(err)
	}
	_, err = q.Add(func(ctx context.Context) {
		err := httpServer(ctx, port2)
		if err != nil {
			panic(err)
		}
	}, "server2")
	if err != nil {
		t.Fatal(err)
	}

	// give the servers some time to start
	time.Sleep(300 * time.Millisecond)

	// test that they are reachable by http
	err = testHttpRequest(port1)
	if err != nil {
		t.Errorf("server should be stoped after shutdown")
	}
	err = testHttpRequest(port2)
	if err != nil {
		t.Errorf("unable to contact server: %v", err)
	}

	err = q.ShutDown(context.Background())
	if err != nil {
		t.Errorf("unable to contact server: %v", err)
	}

	// test that they are NOT reachable by http after shutdown
	err = testHttpRequest(port1)
	if err == nil {
		t.Errorf("server should be stoped after shutdown")
	}
	err = testHttpRequest(port2)
	if err == nil {
		t.Errorf("server should be stoped after shutdown")
	}

}

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
