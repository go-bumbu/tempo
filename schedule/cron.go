package schedule

import (
	"errors"
	"fmt"
	"strings"

	"github.com/reugn/go-quartz/quartz"
)

// NormalizeCron converts a 5-field Unix cron expression
// (minute hour day-of-month month day-of-week) into the 6-field Quartz form
// (second minute hour day-of-month month day-of-week) by prepending a zero
// seconds field. Expressions that already carry 6 or more fields are returned
// trimmed but otherwise unchanged. go-quartz requires the Quartz form.
func NormalizeCron(expr string) string {
	expr = strings.TrimSpace(expr)
	if len(strings.Fields(expr)) == 5 {
		return "0 " + expr
	}
	return expr
}

// ValidateCron reports whether expr is a cron expression the Scheduler can use.
// It accepts both the 5-field Unix and 6-field Quartz forms. Call it before
// persisting user input; Scheduler.Create and Scheduler.Update call it too, so
// an invalid expression can never reach the store.
func ValidateCron(expr string) error {
	if strings.TrimSpace(expr) == "" {
		return errors.New("schedule: cron expression is required")
	}
	if _, err := quartz.NewCronTrigger(NormalizeCron(expr)); err != nil {
		return fmt.Errorf("schedule: invalid cron expression %q: %w", expr, err)
	}
	return nil
}
