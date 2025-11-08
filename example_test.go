package tempo_test

import (
	"github.com/go-bumbu/tempo"
)

var _ = tempo.Task{}

// example about running a task that runs in the background and does data backfilling
func ExampleRunner_data_backfilling() {

	r := tempo.New(1, 1)
	_ = r

	// Output: olleh
}
