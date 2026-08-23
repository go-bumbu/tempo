package filelog_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/go-bumbu/tempo"
	"github.com/go-bumbu/tempo/filelog"
)

// ExampleStore wires a filelog sink into a runner and reads a task's logs back.
// It mirrors ExampleMemTaskLogSink's Sleep+ShutDown pattern so the goroutines are
// cleaned up and the output is deterministic.
func ExampleStore() {
	dir, _ := os.MkdirTemp("", "tempo-filelog-*")
	defer func() { _ = os.RemoveAll(dir) }()

	sink, _ := filelog.New(filelog.Config{Dir: dir})
	r, _ := tempo.NewQueueRunner(tempo.RunnerCfg{
		Parallelism: 1, QueueSize: 10,
		Persistence: tempo.NewMemPersistence(),
		LogSink:     sink, LogLevel: slog.LevelInfo,
	})
	r.RegisterRaw("greet", func(_ context.Context, log *slog.Logger, _ tempo.Progress, _ []byte) error {
		log.Info("hello", "who", "world")
		return nil
	})
	r.StartBg()
	id, _, _ := r.AddRaw("greet", nil)

	time.Sleep(100 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = r.ShutDown(ctx)

	entries, _ := sink.Logs(context.Background(), id)
	for _, e := range entries {
		fmt.Printf("%s %s\n", e.Level, e.Message)
	}
	// Output:
	// INFO task started
	// INFO hello who=world
	// INFO task finished
}
