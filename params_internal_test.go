package tempo

import (
	"context"
	"log/slog"
	"testing"
)

func TestWithExclusionGroupStoresGroup(t *testing.T) {
	r, err := NewQueueRunner(RunnerCfg{Parallelism: 1, Persistence: NewMemPersistence()})
	if err != nil {
		t.Fatal(err)
	}
	r.RegisterRaw("scan", func(ctx context.Context, log *slog.Logger, prog Progress, p []byte) error {
		return nil
	}, WithExclusionGroup("files"))

	def, ok := r.registry.lookup("scan")
	if !ok {
		t.Fatal("task not registered")
	}
	if def.group != "files" {
		t.Fatalf("registered group = %q, want %q", def.group, "files")
	}
}
