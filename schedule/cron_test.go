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
		{name: "surrounding whitespace is trimmed", in: "  30 4 * * 1  ", want: "0 30 4 * * 1"},
		{name: "6-field quartz cron is unchanged", in: "0 0 2 * * *", want: "0 0 2 * * *"},
		{name: "7-field cron with year is unchanged", in: "0 0 2 * * * 2026", want: "0 0 2 * * * 2026"},
		{name: "empty stays empty", in: "", want: ""},
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
}
