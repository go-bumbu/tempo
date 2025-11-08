package tempo

import (
	"fmt"
	"testing"
	"time"
)

func TestRun(t *testing.T) {
	t.Skip("tmep")
	// Create a persistent task runner with 3 workers and queue size 10
	runner := New(1, 10)

	// OneShot tasks dynamically
	for i := 1; i <= 5; i++ {
		id := i
		runner.OneShot(Task{
			ID:      id,
			Message: fmt.Sprintf("Dynamic task %d", id),
			Run: func() {
				time.Sleep(500 * time.Millisecond) // simulate work
			},
		})
	}

	// Simulate waiting for more tasks to arrive
	time.Sleep(2 * time.Second)

	// Stop the runner gracefully
	runner.Stop()
	fmt.Println("All workers stopped")
}
