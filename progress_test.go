package tempo

import (
	"math"
	"testing"
	"time"
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
