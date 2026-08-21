package schedule

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/reugn/go-quartz/quartz"
)

// NormalizeCron converts a 5-field Unix cron expression
// (minute hour day-of-month month day-of-week) into the 6-field Quartz form
// (second minute hour day-of-month month day-of-week) by prepending a zero
// seconds field and translating the day-of-week field. Unix uses 0=Sunday…6=Saturday
// (7 also Sunday), while Quartz uses 1=Sunday…7=Saturday. Expressions that already
// carry 6 or more fields are returned trimmed but otherwise unchanged.
// go-quartz requires the Quartz form.
//
// The day-of-week translation covers what Unix cron itself can express: plain
// numbers, lists, ranges and steps, with named days and wildcards left alone.
// Quartz-only tokens — "L" for the last such day of the month, "5L", "1#2" for
// the second occurrence — are passed through as-is, so a 5-field expression
// carrying one keeps Quartz's numbering rather than Unix's: "0 3 * * 1#2" fires
// on the second Sunday, not the second Monday a Unix reader would expect. Write
// the full 6-field Quartz form when using those tokens.
func NormalizeCron(expr string) string {
	expr = strings.TrimSpace(expr)
	fields := strings.Fields(expr)
	if len(fields) == 5 {
		fields[4] = translateDayOfWeek(fields[4])
		return "0 " + strings.Join(fields, " ")
	}
	return expr
}

// translateDayOfWeek converts Unix day-of-week values (0-7) to Quartz (1-7).
// Handles single values, lists, ranges, steps, wildcards, and named days.
// Mixed forms like "MON,6" translate the numeric portion while preserving names.
func translateDayOfWeek(field string) string {
	if field == "*" || field == "?" {
		return field
	}
	if strings.HasPrefix(field, "*/") {
		return field
	}

	var stepSuffix string
	base := field
	if idx := strings.Index(field, "/"); idx != -1 {
		base = field[:idx]
		stepSuffix = field[idx:]
	}

	parts := strings.Split(base, ",")
	for i, part := range parts {
		parts[i] = translateDayPart(part)
	}
	return strings.Join(parts, ",") + stepSuffix
}

func translateDayPart(part string) string {
	if idx := strings.Index(part, "-"); idx != -1 {
		start := part[:idx]
		end := part[idx+1:]
		return translateDayNumber(start) + "-" + translateDayNumber(end)
	}
	return translateDayNumber(part)
}

func translateDayNumber(s string) string {
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 || n > 7 {
		return s
	}
	return strconv.Itoa((n % 7) + 1)
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
