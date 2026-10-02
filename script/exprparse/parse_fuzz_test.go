package exprparse

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/Gaurav-Gosain/golars/lazy"
)

// seedCorpus collects every backquoted literal from this package's
// tests, which covers the expressions the unit tests already use.
func seedCorpus(tb testing.TB) []string {
	tb.Helper()
	files, err := filepath.Glob("*_test.go")
	if err != nil {
		tb.Fatal(err)
	}
	re := regexp.MustCompile("`([^`\n]{1,200})`")
	seen := map[string]bool{}
	var out []string
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			tb.Fatal(err)
		}
		for _, m := range re.FindAllStringSubmatch(string(b), -1) {
			if !seen[m[1]] {
				seen[m[1]] = true
				out = append(out, m[1])
			}
		}
	}
	for ns, names := range Functions() {
		for _, name := range names {
			switch ns {
			case "":
				out = append(out, "x."+name+"()", "x."+name+"(1)", name+"(x, y)")
			case "free":
				out = append(out, name+"()", name+"(x)", name+"(x, 1, \"a\")")
			default:
				out = append(out, "name."+ns+"."+name+"()", "ts."+ns+"."+name+"(\"1d\")", ns+"."+name+"(tags, 2)")
			}
		}
	}
	slices.Sort(out)
	return out
}

var expLiteral = regexp.MustCompile(`[0-9.][eE][+-]?[0-9]`)

// evalSafe reports whether evaluating s is cheap enough for the fuzzer:
// large numeric literals could ask for huge ranges or repeats.
func evalSafe(s string) bool {
	digits := 0
	for _, c := range s {
		if c >= '0' && c <= '9' {
			digits++
			if digits > 4 {
				return false
			}
			continue
		}
		digits = 0
	}
	return !expLiteral.MatchString(s)
}

// FuzzParse checks that Parse and GoSource never panic, and that a
// parsed expression evaluates against a small frame without panicking
// (an error is fine).
func FuzzParse(f *testing.F) {
	for _, s := range seedCorpus(f) {
		f.Add(s)
	}
	df := probeFrame(&testing.T{})
	f.Fuzz(func(t *testing.T, s string) {
		e, err := Parse(s)
		if _, gerr := GoSource(s); (gerr == nil) != (err == nil) {
			t.Fatalf("Parse and GoSource disagree on %q: %v vs %v", s, err, gerr)
		}
		if err != nil || !evalSafe(s) {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		out, err := lazy.FromDataFrame(df).Select(e.Alias("out")).Collect(ctx)
		if err == nil {
			out.Release()
		}
	})
}
