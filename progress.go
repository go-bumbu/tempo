package tempo

import (
	"time"
)

// ProgressState is the latest reported progress for one task. Progress is
// last-value-wins: each update replaces the previous state. Percent and ETA are
// computed from it and never stored, so they cannot drift.
type ProgressState struct {
	Done      int64
	Total     int64
	Stage     string
	UpdatedAt time.Time
}

// Percent reports Done/Total clamped to [0,1]. ok is false when Total <= 0.
func (p ProgressState) Percent() (float64, bool) {
	if p.Total <= 0 {
		return 0, false
	}
	f := float64(p.Done) / float64(p.Total)
	if f < 0 {
		f = 0
	}
	if f > 1 {
		f = 1
	}
	return f, true
}

// ETA estimates the time remaining by linear extrapolation from startedAt to
// UpdatedAt. ok is false when it cannot be computed — no Total, no Done yet, a
// zero startedAt, or a non-positive elapsed — and it is (0, true) once
// Done >= Total.
func (p ProgressState) ETA(startedAt time.Time) (time.Duration, bool) {
	if p.Total <= 0 || p.Done <= 0 || startedAt.IsZero() {
		return 0, false
	}
	if p.Done >= p.Total {
		return 0, true
	}
	elapsed := p.UpdatedAt.Sub(startedAt)
	if elapsed <= 0 {
		return 0, false
	}
	remaining := time.Duration(float64(elapsed) * float64(p.Total-p.Done) / float64(p.Done))
	return remaining, true
}
