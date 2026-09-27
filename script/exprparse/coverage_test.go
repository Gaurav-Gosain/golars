package exprparse

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/script"
	"github.com/Gaurav-Gosain/golars/series"
)

// Every function call shown in the `with` command's hover docs, and
// every `with` and `filter` example, must parse. The docs are the
// user-facing contract, so a rename in the parser without a docs
// update (or the reverse) fails here.
func TestWithDocMethodsParse(t *testing.T) {
	long := script.FindCommand("with").LongDoc
	sections := map[string]string{}
	for _, m := range regexp.MustCompile(`(?s)\*\*(.+?)\*\*\n(.*?)(?:\n\n|$)`).FindAllStringSubmatch(long, -1) {
		sections[m[1]] = m[2]
	}
	calls := regexp.MustCompile("`([^`]+\\))`").FindAllStringSubmatch(sections["Common functions"], -1)
	if len(calls) < 30 {
		t.Fatalf("found %d calls in the Common functions section, want at least 30", len(calls))
	}
	for _, c := range calls {
		if _, err := Parse(c[1]); err != nil {
			t.Errorf("%s: %v", c[1], err)
		}
	}
	for _, ex := range script.FindCommand("with").Examples {
		_, rhs, _ := strings.Cut(ex, "=")
		if _, err := Parse(rhs); err != nil {
			t.Errorf("%s: %v", ex, err)
		}
	}
	for _, ex := range script.FindCommand("filter").Examples {
		if _, err := Parse(strings.TrimPrefix(ex, "filter ")); err != nil {
			t.Errorf("%s: %v", ex, err)
		}
	}
}

func TestParseErrors(t *testing.T) {
	cases := map[string]string{
		`x.frobnicate()`:     `unknown method "frobnicate"`,
		`x.str.frobnicate()`: "frobnicate",
		`frobnicate(x)`:      `unknown function "frobnicate"`,
		`x.str`:              ".str must be followed by a method",
		`x.shift("a")`:       "shift",
		`x.cast("i128")`:     "i128",
		`x.rolling_mean(3)`:  "rolling_mean",
		`"unterminated`:      "",
		`a + `:               "",
		`a b`:                `unexpected identifier "b"`,
		`(a + 1`:             "",
		`coalesce()`:         "coalesce requires at least 1 argument",
		`lit(a)`:             "expected a literal",
		`x.between(1)`:       "between takes 2 arguments",
		`x.over(1)`:          "over",
		`col(1)`:             "col",
		`x.str.replace("a")`: "replace",
	}
	for src, want := range cases {
		_, err := Parse(src)
		if err == nil {
			t.Errorf("%s: want error", src)
			continue
		}
		if !strings.Contains(err.Error(), want) {
			t.Errorf("%s: error %q does not mention %q", src, err, want)
		}
	}
}

// Parsed expressions evaluate to the values polars 1.39 produces for
// the same expression.
func TestParsedExpressionsEvaluate(t *testing.T) {
	ctx := context.Background()
	a, _ := series.FromInt64("a", []int64{1, 2, 3, 4}, nil)
	b, _ := series.FromFloat64("b", []float64{1.5, 0, -2, 4}, []bool{true, false, true, true})
	s, _ := series.FromString("s", []string{"alpha", "beta", "Gamma", "delta"}, nil)
	df, err := dataframe.New(a, b, s)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	cases := []struct {
		src  string
		want string
	}{
		{`a * 2 + 1`, "[3 5 7 9]"},
		{`-a`, "[-1 -2 -3 -4]"},
		{`a > 2 and a < 4`, "[false false true false]"},
		{`not (a == 1)`, "[false true true true]"},
		{`s.str.upper()`, "[ALPHA BETA GAMMA DELTA]"},
		{`s.str.starts_with("a")`, "[true false false false]"},
		{`s.str.len_chars()`, "[5 4 5 5]"},
		{`b.fill_null(0.5)`, "[1.5 0.5 -2 4]"},
		{`coalesce(b, lit(9.0))`, "[1.5 9 -2 4]"},
		{`a.cum_sum()`, "[1 3 6 10]"},
		{`a.shift(1)`, "[<nil> 1 2 3]"},
		{`a.between(2, 3)`, "[false true true false]"},
		{`a.sum()`, "[10]"}, // select keeps one row, as in polars
		{`b.is_null()`, "[false true false false]"},
	}
	for _, tc := range cases {
		t.Run(tc.src, func(t *testing.T) {
			e, err := Parse(tc.src)
			if err != nil {
				t.Fatal(err)
			}
			out, err := lazy.FromDataFrame(df).Select(e.Alias("out")).Collect(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer out.Release()
			col, err := out.Column("out")
			if err != nil {
				t.Fatal(err)
			}
			got := make([]any, col.Len())
			rows, err := out.Rows()
			if err != nil {
				t.Fatal(err)
			}
			for i, r := range rows {
				got[i] = r[0]
			}
			if s := sprintAny(got); s != tc.want {
				t.Errorf("got %s, want %s", s, tc.want)
			}
		})
	}
}

func sprintAny(vs []any) string {
	parts := make([]string, len(vs))
	for i, v := range vs {
		if v == nil {
			parts[i] = "<nil>"
			continue
		}
		parts[i] = fmt.Sprint(v)
	}
	return "[" + strings.Join(parts, " ") + "]"
}
