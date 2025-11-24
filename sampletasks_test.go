package tempo_test

import (
	"context"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"net"
	"net/http"
	"time"
)

var _ = spew.Dump // keep spew in go mod

type HttpServerRunner struct {
	Port   int
	server *http.Server
}

func NewHttpServerRunner() *HttpServerRunner {
	port, err := GetFreePort()
	if err != nil {
		panic(err)
	}

	return &HttpServerRunner{
		Port: port,
		server: &http.Server{
			Addr: fmt.Sprintf(":%d", port),
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				fmt.Fprintf(w, "Hello from port %d\n", port)
			}),
		},
	}
}

func (s *HttpServerRunner) Run(ctx context.Context) error {

	// Start server in goroutine
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- s.server.ListenAndServe()
	}()

	// Wait for server to be ready or context cancellation
	select {
	case <-time.After(100 * time.Millisecond):
		// No error after short delay → server started
	case err := <-serverErr:
		return fmt.Errorf("server failed to start: %w", err)
	case <-ctx.Done():
		fmt.Printf("server canceled during starupt: %v", ctx.Err())
		return ctx.Err()
	}

	// Wait for cancellation
	<-ctx.Done()

	// Graceful shutdown with timeout
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown failed: %w", err)
	}

	// Check if ListenAndServe returned an error
	select {
	case err := <-serverErr:
		if err != nil {
			return err
		}
	default:
	}

	return ctx.Err()
}

// GetFreePort asks the kernel for a free open port that is ready to use.
func GetFreePort() (port int, err error) {
	var a *net.TCPAddr
	if a, err = net.ResolveTCPAddr("tcp", "localhost:0"); err == nil {
		var l *net.TCPListener
		if l, err = net.ListenTCP("tcp", a); err == nil {
			defer l.Close()
			return l.Addr().(*net.TCPAddr).Port, nil
		}
	}
	return
}

// ====================================================================================
//

type SampleTaskRunner struct {
	Items          []string
	ProcessedItems []string
}

func (s *SampleTaskRunner) Run(ctx context.Context) error {
	for _, item := range s.Items {
		select {
		case <-ctx.Done():
			// clean up shutting down
			return ctx.Err()
		default:
			s.processItem(ctx, item)
		}
	}
	return nil
}

func (s *SampleTaskRunner) processItem(ctx context.Context, item string) {
	time.Sleep(1 * time.Second)
	s.ProcessedItems = append(s.ProcessedItems, item)
}

func (s *SampleTaskRunner) cleanup(ctx context.Context, item string) {

}
