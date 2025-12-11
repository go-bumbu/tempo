package tempo

import "time"

// Schedule interface avoids to hardcode a cron expression
// Then you can provide multiple implementations:
//   - CronSchedule — parses cron syntax.
//   - FixedInterval — simple every N minutes.
//   - CustomSchedule — user-provided logic.
//
// sample usage Schedule: tempo.Cron("@daily")
type Schedule interface {
	NextRun(after time.Time) time.Time
}

// todo type of schedules: now, interval, cron
