package tempo

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestProgressStatePercent(t *testing.T) {
	tests := []struct {
		name   string
		p      ProgressState
		want   float64
		wantOK bool
	}{
		{"no total", ProgressState{Done: 5, Total: 0}, 0, false},
		{"half", ProgressState{Done: 42, Total: 100}, 0.42, true},
		{"clamp over 100", ProgressState{Done: 150, Total: 100}, 1, true},
	}
	for _, tc := range tests {
		got, ok := tc.p.Percent()
		if ok != tc.wantOK || (ok && math.Abs(got-tc.want) > 1e-9) {
			t.Errorf("%s: Percent() = %v, %v; want %v, %v", tc.name, got, ok, tc.want, tc.wantOK)
		}
	}
}

func TestProgressStateETA(t *testing.T) {
	start := time.Unix(0, 0)

	if got, ok := (ProgressState{Done: 50, Total: 100, UpdatedAt: start.Add(10 * time.Second)}).ETA(start); !ok || got != 10*time.Second {
		t.Fatalf("half-done ETA = %v, %v; want 10s, true", got, ok)
	}
	if _, ok := (ProgressState{Done: 1, Total: 0, UpdatedAt: start.Add(time.Second)}).ETA(start); ok {
		t.Error("no total should give ok=false")
	}
	if _, ok := (ProgressState{Done: 0, Total: 100, UpdatedAt: start.Add(time.Second)}).ETA(start); ok {
		t.Error("no progress should give ok=false")
	}
	if d, ok := (ProgressState{Done: 100, Total: 100, UpdatedAt: start.Add(time.Second)}).ETA(start); !ok || d != 0 {
		t.Errorf("complete ETA = %v, %v; want 0, true", d, ok)
	}
	if _, ok := (ProgressState{Done: 5, Total: 100, UpdatedAt: start.Add(10 * time.Second)}).ETA(time.Time{}); ok {
		t.Error("zero startedAt should give ok=false")
	}
}

func TestMemProgressSinkRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewMemTaskProgressSink()
	a, b := uuid.New(), uuid.New()

	if err := s.Set(ctx, a, ProgressState{Done: 3, Total: 10, Stage: "x", UpdatedAt: time.Unix(1, 0)}); err != nil {
		t.Fatal(err)
	}
	// upsert: the second Set replaces the first
	if err := s.Set(ctx, a, ProgressState{Done: 7, Total: 10, UpdatedAt: time.Unix(2, 0)}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := s.Progress(ctx, a)
	if err != nil || !ok || got.Done != 7 || got.Total != 10 {
		t.Fatalf("Progress(a) = %+v, %v, %v; want Done=7 Total=10", got, ok, err)
	}
	if _, ok, _ := s.Progress(ctx, b); ok {
		t.Fatal("unknown id should give ok=false")
	}

	if err := s.RemoveTasks(ctx, []uuid.UUID{a}); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.Progress(ctx, a); ok {
		t.Fatal("after RemoveTasks, a should be gone")
	}

	_ = s.Set(ctx, b, ProgressState{Done: 1, Total: 2, UpdatedAt: time.Unix(3, 0)})
	if err := s.RetainOnly(ctx, []uuid.UUID{}); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.Progress(ctx, b); ok {
		t.Fatal("after RetainOnly(none), b should be gone")
	}
}

// captureSink records every ProgressState that Set forwards to it.
type captureSink struct {
	mu   sync.Mutex
	last ProgressState
	n    int
}

func (c *captureSink) Set(_ context.Context, _ uuid.UUID, p ProgressState) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.last = p
	c.n++
	return nil
}

func TestSinkReporterForwardsSnapshots(t *testing.T) {
	cs := &captureSink{}
	r := newSinkReporter(cs, uuid.New())
	r.SetTotal(10)
	r.SetStage("work")
	if got := r.Inc(3); got != 3 {
		t.Fatalf("Inc = %d, want 3", got)
	}
	r.Set(5)

	cs.mu.Lock()
	defer cs.mu.Unlock()
	if cs.n != 4 {
		t.Fatalf("Set forwarded %d times, want 4", cs.n)
	}
	if cs.last.Done != 5 || cs.last.Total != 10 || cs.last.Stage != "work" {
		t.Fatalf("last snapshot = %+v; want Done=5 Total=10 Stage=work", cs.last)
	}
	if cs.last.UpdatedAt.IsZero() {
		t.Fatal("snapshot UpdatedAt not stamped")
	}
}

func TestSinkReporterConcurrent(t *testing.T) {
	cs := &captureSink{}
	r := newSinkReporter(cs, uuid.New())
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); r.Inc(1) }()
	}
	wg.Wait()
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if cs.last.Done != 50 {
		t.Fatalf("after 50 concurrent Inc, Done = %d, want 50", cs.last.Done)
	}
}

func TestDiscardProgress(t *testing.T) {
	discardProgress.SetTotal(5)
	discardProgress.Set(3)
	discardProgress.SetStage("x")
	if got := discardProgress.Inc(2); got != 0 {
		t.Fatalf("discard Inc = %d, want 0", got)
	}
}
