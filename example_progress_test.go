package tempo_test

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-bumbu/tempo"
)

// ExampleMemTaskProgressSink shows a job reporting progress through the
// tempo.Progress reporter and a caller reading the final state back with a
// percentage. It mirrors ExampleMemTaskLogSink's Sleep+ShutDown pattern so the
// goroutines are cleaned up and the output is deterministic.
func ExampleMemTaskProgressSink() {
	sink := tempo.NewMemTaskProgressSink()
	r, _ := tempo.NewQueueRunner(tempo.RunnerCfg{
		Parallelism: 1, QueueSize: 10,
		Persistence:  tempo.NewMemPersistence(),
		ProgressSink: sink,
	})

	const Scan = "scan"
	r.RegisterRaw(Scan, func(_ context.Context, _ *slog.Logger, prog tempo.Progress, _ []byte) error {
		prog.SetTotal(4)
		for i := 0; i < 4; i++ {
			prog.Inc(1)
		}
		return nil
	})
	r.StartBg()
	id, _, _ := r.AddRaw(Scan, nil)

	time.Sleep(100 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = r.ShutDown(ctx)

	p, _, _ := sink.Progress(context.Background(), id)
	pct, _ := p.Percent()
	fmt.Printf("%s: %d/%d (%.0f%%)\n", Scan, p.Done, p.Total, pct*100)
	// Output:
	// scan: 4/4 (100%)
}
