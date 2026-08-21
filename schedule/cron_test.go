package schedule

import (
	"strings"
	"testing"
)

func TestNormalizeCron(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "5-field unix cron gains a seconds field", in: "0 2 * * *", want: "0 0 2 * * *"},
		{name: "surrounding whitespace is trimmed", in: "  30 4 * * 1  ", want: "0 30 4 * * 2"},
		{name: "6-field quartz cron is unchanged", in: "0 0 2 * * *", want: "0 0 2 * * *"},
		{name: "6-field with numeric day unchanged", in: "0 0 3 * * 1", want: "0 0 3 * * 1"},
		{name: "7-field cron with year is unchanged", in: "0 0 2 * * * 2026", want: "0 0 2 * * * 2026"},
		{name: "empty stays empty", in: "", want: ""},

		// Day-of-week translation from Unix (0-7) to Quartz (1-7)
		{name: "unix sunday 0 becomes quartz 1", in: "0 3 * * 0", want: "0 0 3 * * 1"},
		{name: "unix monday 1 becomes quartz 2", in: "0 3 * * 1", want: "0 0 3 * * 2"},
		{name: "unix saturday 6 becomes quartz 7", in: "0 3 * * 6", want: "0 0 3 * * 7"},
		{name: "unix sunday 7 becomes quartz 1", in: "0 3 * * 7", want: "0 0 3 * * 1"},

		// Lists
		{name: "day list is translated", in: "0 3 * * 1,3,5", want: "0 0 3 * * 2,4,6"},
		{name: "list with sunday 0", in: "0 3 * * 0,2,4", want: "0 0 3 * * 1,3,5"},

		// Ranges
		{name: "weekday range mon-fri", in: "0 3 * * 1-5", want: "0 0 3 * * 2-6"},
		{name: "range starting at sunday 0", in: "0 3 * * 0-3", want: "0 0 3 * * 1-4"},

		// Steps on ranges
		{name: "step on weekday range", in: "0 3 * * 1-5/2", want: "0 0 3 * * 2-6/2"},

		// Wildcards and steps on wildcards
		{name: "wildcard unchanged", in: "*/15 * * * *", want: "0 */15 * * * *"},
		{name: "step on wildcard day unchanged", in: "0 3 * * */2", want: "0 0 3 * * */2"},
		{name: "question mark unchanged", in: "0 3 * * ?", want: "0 0 3 * * ?"},

		// Named days
		{name: "uppercase name unchanged", in: "0 3 * * MON", want: "0 0 3 * * MON"},
		{name: "lowercase name unchanged", in: "0 3 * * sun", want: "0 0 3 * * sun"},
		{name: "name range unchanged", in: "0 3 * * MON-FRI", want: "0 0 3 * * MON-FRI"},
		{name: "name list unchanged", in: "0 3 * * Mon,Wed,Fri", want: "0 0 3 * * Mon,Wed,Fri"},

		// Mixed numeric and named days
		{name: "name then number translates number", in: "0 3 * * MON,6", want: "0 0 3 * * MON,7"},
		{name: "number then name translates number", in: "0 3 * * 6,MON", want: "0 0 3 * * 7,MON"},

		// Out-of-range values pass through untranslated (will be rejected by go-quartz)
		{name: "out-of-range 8 unchanged", in: "0 3 * * 8", want: "0 0 3 * * 8"},
		{name: "out-of-range 14 unchanged", in: "0 3 * * 14", want: "0 0 3 * * 14"},
		{name: "out-of-range 100 unchanged", in: "0 3 * * 100", want: "0 0 3 * * 100"},
		{name: "out-of-range bound left untranslated, in-range bound translated", in: "0 3 * * 0-8", want: "0 0 3 * * 1-8"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := NormalizeCron(tc.in); got != tc.want {
				t.Errorf("NormalizeCron(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestValidateCron(t *testing.T) {
	t.Run("accepts both cron widths", func(t *testing.T) {
		for _, expr := range []string{"0 2 * * *", "0 0 2 * * *", "*/15 * * * *", "0 */5 * * * *"} {
			if err := ValidateCron(expr); err != nil {
				t.Errorf("ValidateCron(%q) = %v, want nil", expr, err)
			}
		}
	})

	t.Run("accepts unix sunday 0", func(t *testing.T) {
		if err := ValidateCron("0 3 * * 0"); err != nil {
			t.Errorf("ValidateCron(%q) = %v, want nil", "0 3 * * 0", err)
		}
	})

	t.Run("rejects an empty expression", func(t *testing.T) {
		err := ValidateCron("   ")
		if err == nil {
			t.Fatal("expected an error for a blank expression")
		}
		if !strings.Contains(err.Error(), "required") {
			t.Errorf("expected the error to say the expression is required, got %v", err)
		}
	})

	t.Run("rejects nonsense and names the expression", func(t *testing.T) {
		for _, expr := range []string{"not a cron", "99 * * * *", "* *"} {
			err := ValidateCron(expr)
			if err == nil {
				t.Errorf("ValidateCron(%q) = nil, want an error", expr)
				continue
			}
			if !strings.Contains(err.Error(), expr) {
				t.Errorf("expected error to quote %q, got %v", expr, err)
			}
		}
	})

	t.Run("rejects out-of-range day-of-week values", func(t *testing.T) {
		for _, expr := range []string{"0 3 * * 8", "0 3 * * 14", "0 3 * * 100", "0 3 * * 0-8"} {
			err := ValidateCron(expr)
			if err == nil {
				t.Errorf("ValidateCron(%q) = nil, want an error for out-of-range day", expr)
			}
		}
	})
}

func TestNormalizeCronIsIdempotent(t *testing.T) {
	tests := []string{
		"0 2 * * *",
		"0 3 * * 0",
		"0 3 * * 1",
		"0 3 * * 6",
		"0 3 * * 7",
		"0 3 * * 1,3,5",
		"0 3 * * 0,2,4",
		"0 3 * * 1-5",
		"0 3 * * 0-3",
		"0 3 * * 1-5/2",
		"*/15 * * * *",
		"0 3 * * */2",
		"0 3 * * ?",
		"0 3 * * MON",
		"0 3 * * sun",
		"0 3 * * MON-FRI",
		"0 3 * * Mon,Wed,Fri",
		"0 3 * * MON,6",
		"0 3 * * 6,MON",
		"0 0 2 * * *",
		"0 0 3 * * 1",
	}
	for _, expr := range tests {
		t.Run(expr, func(t *testing.T) {
			first := NormalizeCron(expr)
			second := NormalizeCron(first)
			if first != second {
				t.Errorf("NormalizeCron is not idempotent for %q: first=%q, second=%q", expr, first, second)
			}
		})
	}
}
