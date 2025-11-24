package tempo_test

import (
	"github.com/go-bumbu/tempo"
	"testing"
	"time"
)

func TestRunner(t *testing.T) {
	srv := tempo.NewHttpServerRunner()

	pool := tempo.NewTaskRunner()
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
}
