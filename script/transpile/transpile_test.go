package transpile

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Each input script must produce Go source that (a) parses, (b) has
// no unused imports, and (c) contains the expected calls. We do not
// execute the generated program here - the example inputs reference
// CSV paths relative to the repo root that may not exist in every
// test environment.
func TestTranspileExamples(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		script string
		want   []string // substrings that must appear in the generated source
	}{
		{
			name: "derived",
			script: `load data/salaries.csv
with upper = name.str.upper()
with monthly = amount / 12
head 5`,
			want: []string{
				`golars.ReadCSV("data/salaries.csv"`,
				`.Str().ToUpper().Alias("upper")`,
				`.Div(expr.LitInt64(12)).Alias("monthly")`,
				`.Limit(5)`,
			},
		},
		{
			name: "filter_like",
			script: `load data/people.csv
filter name like "a%"
show`,
			want: []string{
				`.Str().Like("a%")`,
				`fmt.Println(out)`,
			},
		},
		{
			name: "rolling_ewm",
			script: `load data/prices.csv
with ma = price.rolling_mean(3, 1)
with smooth = price.ewm_mean(0.3)`,
			want: []string{
				`.RollingMean(`,
				`.EWMMean(`,
			},
		},
		{
			name: "expressions_and_time_series",
			script: `load data/rates.csv as rates
load data/events.csv
with year = dt.year(ts)
with band = cut(amount, [10, 100], labels=["s", "m", "l"])
filter user in ["a", "b"] and amount // 2 > 1
select ts, user, amount, share = amount / amount.sum()
show
reset
join_asof rates on ts by user forward tolerance 2h
join rates on ts,user full suffix _r
sort user asc ts desc
group_by_dynamic ts every 1h by user amount:sum:total n=len()
to_dummies user`,
			want: []string{
				`expr.Col("ts").Dt().Year().Alias("year")`,
				`.Cut([]float64{10, 100}, expr.CutOptions{Labels: []string{"s", "m", "l"}})`,
				`.IsIn("a", "b")`,
				`.FloorDiv(expr.LitInt64(2))`,
				`expr.Col("amount").Div(expr.Col("amount").Sum()).Alias("share")`,
				`JoinAsof(rates, dataframe.AsofOptions{On: "ts", By: []string{"user"}, Strategy: dataframe.AsofForward, Tolerance: "2h"})`,
				`Join(rates, []string{"ts", "user"}, dataframe.FullJoin, dataframe.WithJoinSuffix("_r"))`,
				`SortBy([]string{"user", "ts"}, []compute.SortOptions{{Descending: false}, {Descending: true}})`,
				`GroupByDynamic("ts", dataframe.DynamicGroupOptions{Every: "1h", GroupBy: []string{"user"}})`,
				`expr.Len().Alias("n")`,
				`ToDummies(ctx, dataframe.ToDummiesOptions{Columns: []string{"user"}})`,
			},
		},
		{
			name:   "no_show_prunes_fmt",
			script: `load data/x.csv`,
			want:   []string{`golars.ReadCSV`},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			in := filepath.Join(dir, "script.glr")
			if err := os.WriteFile(in, []byte(tc.script), 0o644); err != nil {
				t.Fatal(err)
			}
			var out strings.Builder
			if err := Transpile(in, &out, "main"); err != nil {
				t.Fatalf("transpile: %v", err)
			}
			src := out.String()
			// Must parse as valid Go.
			if _, err := parser.ParseFile(token.NewFileSet(), "out.go", src, 0); err != nil {
				t.Fatalf("generated code does not parse:\n%s\n\nerror: %v", src, err)
			}
			// No import should be unused: pruneAndFormat strips those,
			// so any remaining import line must actually be referenced.
			if strings.Contains(src, `"fmt"`) && !strings.Contains(src, "fmt.") {
				t.Errorf("fmt imported but never used:\n%s", src)
			}
			for _, substr := range tc.want {
				if !strings.Contains(src, substr) {
					t.Errorf("output missing %q\n\n%s", substr, src)
				}
			}
		})
	}
}

func TestTranspileUnknownCommandLeavesTODO(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	in := filepath.Join(dir, "s.glr")
	os.WriteFile(in, []byte("wibble foo bar\n"), 0o644)
	var out strings.Builder
	if err := Transpile(in, &out, "main"); err != nil {
		t.Fatalf("transpile: %v", err)
	}
	if !strings.Contains(out.String(), `TODO(glr): command "wibble"`) {
		t.Errorf("unknown command should emit TODO comment; got:\n%s", out.String())
	}
}
