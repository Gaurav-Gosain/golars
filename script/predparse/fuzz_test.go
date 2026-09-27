package predparse

import (
	"errors"
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/script/exprparse"
)

// FuzzPredicate checks that no predicate panics the parser, that
// errors carry a span inside the input, and that GoSource agrees with
// Parse.
func FuzzPredicate(f *testing.F) {
	for _, s := range []string{
		`age >= 30 and dept == "eng"`, `x is_null`, `name contains "foo"`, `brand like "%COPPER"`,
		`status in ["open", "held"]`, `dt.year(ts) == 2024 or str.len_chars(name) > 10`, `active`,
		`not (a < 1)`, `1`, `a = 1`, `x not in [1,`,
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		_, err := Parse(s)
		_, gerr := GoSource(s)
		if (err == nil) != (gerr == nil) {
			t.Fatalf("%q: Parse error %v, GoSource error %v", s, err, gerr)
		}
		if err == nil {
			return
		}
		var pe *exprparse.Error
		if errors.As(err, &pe) {
			if pe.Pos < 0 || pe.End < pe.Pos || pe.End > len(s) {
				t.Fatalf("%q: span [%d,%d) outside the input", s, pe.Pos, pe.End)
			}
		} else if !strings.Contains(err.Error(), "constant") {
			t.Fatalf("%q: unpositioned error %v", s, err)
		}
		if strings.Contains(err.Error(), "internal:") {
			t.Fatalf("%q: %v", s, err)
		}
	})
}
